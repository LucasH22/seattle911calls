'use strict';

const http = require('http');
const assert = require('assert');
const express = require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const path = require('path');
const hbase = require('hbase');

const port = Number(process.argv[2] || 3017);
const hbaseUrlString = process.argv[3] || 'http://ec2-54-89-237-222.compute-1.amazonaws.com:8070';
const url = new URL(hbaseUrlString);

// HBase REST client
const hclient = hbase({
    host: url.hostname,
    path: url.pathname ?? "/",
    port: url.port,
    protocol: url.protocol.slice(0, -1),
    encoding: 'latin1',
    auth: process.env.HBASE_AUTH
});

// Converts HBase cell to a JS number
function counterToNumber(c) {
    if (c == null) return NaN;

    // If it's already a number, just use it
    if (typeof c === 'number') {
        return c;
    }

    // If it's a string, try plain numeric
    if (typeof c === 'string') {
        const trimmed = c.trim();
        const asNum = Number(trimmed);
        if (!Number.isNaN(asNum)) {
            return asNum;
        }

        // Otherwise treat it as latin1-encoded binary (8-byte HBase counter)
        const buf = Buffer.from(c, 'latin1');
        if (buf.length === 8) {
            return Number(buf.readBigInt64BE());
        }
        return NaN;
    }

    return NaN;
}

// Turns a row of HBase cells into a map: "family:qualifier" -> number
function rowToMap(row) {
    const stats = {};
    row.forEach(function (item) {
        const col = item['column'];
        const n = counterToNumber(item['$']);
        if (!Number.isNaN(n)) {
            stats[col] = n;
        }
    });
    return stats;
}

// Extract stats from the batch table row (lucashou_fire_calls_by_type)
function statsFromBatch(cells) {
    const m = rowToMap(cells);
    return {
        total_calls: m['calls:total_calls'] || 0,
        day_calls:   m['calls:day_calls']   || 0,
        night_calls: m['calls:night_calls'] || 0
    };
}

// Extract stats from the speed table row (lucashou_fire_calls_by_type_speed)
function statsFromSpeed(cells) {
    if (!cells || cells.length === 0) {
        return { day: 0, night: 0 };
    }
    const m = rowToMap(cells);
    return {
        day:   m['calls:day']   || 0,
        night: m['calls:night'] || 0
    };
}

// Convert recent-incident HBase row (lucashou_fire911_recent) into a JS object
function recentRowToObj(rowKey, cells) {
    const obj = { key: rowKey };
    cells.forEach(item => {
        const col = item.column;
        const valRaw = item['$'];
        const val = Buffer.isBuffer(valRaw) ? valRaw.toString('utf8') : String(valRaw);

        if (col === 'loc:incident_number') {
            obj.incident_number = val;
        } else if (col === 'loc:type') {
            obj.type = val;
        } else if (col === 'loc:datetime') {
            obj.datetime = val;
        } else if (col === 'loc:lat') {
            obj.lat = parseFloat(val);
        } else if (col === 'loc:lon') {
            obj.lon = parseFloat(val);
        }
    });
    return obj;
}

// Test read from batch table at startup
hclient.table('lucashou_fire_calls_by_type')
    .row('Aid Response')
    .get((error, value) => {
        if (error) {
            console.error('Error reading test row from HBase:', error);
        } else if (!value || value.length === 0) {
            console.warn('No HBase row found for test key "Aid Response"');
        } else {
            console.log('Sample row from lucashou_fire_calls_by_type:');
            console.log(rowToMap(value));
        }
    });

// Serve static files from ./public
app.use(express.static('public'));

// Dynamic endpoint: /callsbytype.html -> merged batch + speed stats for a given type
app.get('/callsbytype.html', function (req, res) {
    const rawType = req.query['type'] || '';
    const callType = rawType.trim();

    console.log('Querying 911 stats for type:', callType);

    if (!callType) {
        res.status(400).send('Missing call type');
        return;
    }

    // Defaults in case HBase has no row or errors out
    let batchStats = {
        total_calls: 0,
        day_calls:   0,
        night_calls: 0
    };

    // 1) Read batch table (full history up to last batch)
    hclient
        .table('lucashou_fire_calls_by_type')
        .row(callType)
        .get(function (err, batchCells) {
            if (err) {
                console.error('HBase error (batch):', err);
            } else if (batchCells && batchCells.length > 0) {
                batchStats = statsFromBatch(batchCells);
            } else {
                console.log('No batch row for', callType, 'in lucashou_fire_calls_by_type; treating as zeros.');
            }

            // 2) Read speed table (deltas since last batch)
            hclient
                .table('lucashou_fire_calls_by_type_speed_v2')
                .row(callType)
                .get(function (err2, speedCells) {
                    let speedStats = { day: 0, night: 0 };

                    if (err2) {
                        console.error('HBase error (speed):', err2);
                    } else {
                        speedStats = statsFromSpeed(speedCells || []);
                    }

                    // 3) Merge batch + speed
                    const dayTotal   = batchStats.day_calls   + speedStats.day;
                    const nightTotal = batchStats.night_calls + speedStats.night;

                    const totalCalls =
                        batchStats.total_calls > 0
                            ? batchStats.total_calls + speedStats.day + speedStats.night
                            : dayTotal + nightTotal;

                    // 4) Render Mustache template
                    const template = filesystem.readFileSync('result.mustache').toString();
                    const html = mustache.render(template, {
                        type: callType,

                        // merged live totals
                        total_calls: totalCalls,
                        day_calls:   dayTotal,
                        night_calls: nightTotal,
                    });

                    res.send(html);
                });
        });
});

// View all call types (batch + speed layer, live)
app.get('/alltypes.html', (req, res) => {
    const batchTable = 'lucashou_fire_calls_by_type';
    const speedTable = 'lucashou_fire_calls_by_type_speed_v2';

    // Aggregated stats from each table
    const batch = {};
    const speed = {};

    // Group an array of cells (from scan) by row key
    function groupCellsByKey(cells) {
        const grouped = new Map();
        (cells || []).forEach(cell => {
            const key = Buffer.isBuffer(cell.key)
                ? cell.key.toString('utf8')
                : String(cell.key);

            if (!grouped.has(key)) {
                grouped.set(key, []);
            }

            grouped.get(key).push({
                column: cell.column,
                $: cell.$
            });
        });
        return grouped;
    }

    // 1) Scan batch table: calls:total_calls, calls:day_calls, calls:night_calls
    hclient
        .table(batchTable)
        .scan({ maxVersions: 1 }, (err, cells) => {
            if (err) {
                console.error('Error scanning batch table:', err);
                res.status(500).send('Error scanning lucashou_fire_calls_by_type');
                return;
            }

            const grouped = groupCellsByKey(cells);

            grouped.forEach((cellArray, type) => {
                const m = rowToMap(cellArray);
                const dayCalls   = m['calls:day_calls']   || 0;
                const nightCalls = m['calls:night_calls'] || 0;
                const totalCalls = m['calls:total_calls'] || (dayCalls + nightCalls);

                batch[type] = {
                    total: totalCalls,
                    day:   dayCalls,
                    night: nightCalls
                };
            });

            // 2) Scan speed table: calls:day, calls:night
            hclient
                .table(speedTable)
                .scan({ maxVersions: 1 }, (err2, cells2) => {
                    if (err2) {
                        console.error('Error scanning speed table:', err2);
                        res.status(500).send('Error scanning lucashou_fire_calls_by_type_speed_v2');
                        return;
                    }

                    const groupedSpeed = groupCellsByKey(cells2);

                    groupedSpeed.forEach((cellArray, type) => {
                        const m = rowToMap(cellArray);
                        const day   = m['calls:day']   || 0;
                        const night = m['calls:night'] || 0;

                        speed[type] = { day, night };
                    });

                    // 3) Merge batch + speed into a single list
                    const allTypes = new Set([
                        ...Object.keys(batch),
                        ...Object.keys(speed)
                    ]);

                    const resultRows = [];

                    allTypes.forEach(type => {
                        const b = batch[type] || { total: 0, day: 0, night: 0 };
                        const s = speed[type] || { day: 0, night: 0 };

                        const day   = b.day + s.day;
                        const night = b.night + s.night;
                        const baseTotal = b.total || (b.day + b.night);
                        const total = baseTotal + s.day + s.night;

                        resultRows.push({
                            type,
                            total_calls: total,
                            day_calls:   day,
                            night_calls: night
                        });
                    });

                    // Sort by total desc so most common types float to the top
                    resultRows.sort((a, b) => b.total_calls - a.total_calls);

                    const view = { rows: resultRows };

                    filesystem.readFile(
                        path.join(__dirname, 'alltypes.mustache'),
                        'utf8',
                        (err3, tmpl) => {
                            if (err3) {
                                console.error('Error reading alltypes.mustache:', err3);
                                res.status(500).send('Unable to load type list template');
                                return;
                            }

                            const html = mustache.render(tmpl, view);
                            res.send(html);
                        }
                    );
                });
        });
});


// JSON endpoint: 10 most recent incidents for the map
app.get('/recent.json', function (req, res) {
    // Fixed row keys pos00..pos09
    const keys = [];
    for (let i = 0; i < 10; i++) {
        keys.push(`pos${i.toString().padStart(2, '0')}`);
    }

    const results = new Array(keys.length);
    let remaining = keys.length;
    let responded = false;

    keys.forEach((rk, index) => {
        hclient
            .table('lucashou_fire911_recent_v2')
            .row(rk)
            .get(function (err, cells) {
                if (responded) {
                    return;
                }

                if (err) {
                    console.error('HBase error reading recent row', rk, err);
                    responded = true;
                    res.status(500).send('Error querying recent incidents');
                    return;
                }

                if (cells && cells.length > 0) {
                    results[index] = recentRowToObj(rk, cells);
                } else {
                    results[index] = null;
                }

                remaining -= 1;
                if (remaining === 0 && !responded) {
                    const filtered = results.filter(x => x);
                    res.json(filtered);
                }
            });
    });
});

// Start webserver
app.listen(port, () => {
    console.log(`911 web app listening on port ${port}`);
    console.log(`Using HBase REST at ${hbaseUrlString}`);
});