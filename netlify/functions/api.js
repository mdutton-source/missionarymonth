const { neon } = require("@neondatabase/serverless");

function getDb() {
  return neon(process.env.NETLIFY_DATABASE_URL);
}

async function initDb(sql) {
  await sql`CREATE TABLE IF NOT EXISTS missionaries (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE, zone TEXT NOT NULL, created_at TIMESTAMP DEFAULT NOW())`;
  await sql`CREATE TABLE IF NOT EXISTS daily_entries (id SERIAL PRIMARY KEY, missionary_name TEXT NOT NULL, date_key TEXT NOT NULL, habits JSONB NOT NULL DEFAULT '{}', submitted_at TIMESTAMP DEFAULT NOW(), UNIQUE(missionary_name, date_key))`;
  await sql`CREATE TABLE IF NOT EXISTS weekly_entries (id SERIAL PRIMARY KEY, missionary_name TEXT NOT NULL, week_key TEXT NOT NULL, challenges JSONB NOT NULL DEFAULT '{}', submitted_at TIMESTAMP DEFAULT NOW(), UNIQUE(missionary_name, week_key))`;
  await sql`CREATE TABLE IF NOT EXISTS bonus_entries (id SERIAL PRIMARY KEY, missionary_name TEXT NOT NULL UNIQUE, bonuses JSONB NOT NULL DEFAULT '{}', submitted_at TIMESTAMP DEFAULT NOW())`;

  // One-time: merge "Annika" into "Annika Ellefsen"
  const annikaSrc = await sql`SELECT * FROM daily_entries WHERE LOWER(missionary_name) = 'annika'`;
  for (const row of annikaSrc) {
    await sql`INSERT INTO daily_entries (missionary_name, date_key, habits) VALUES ('Annika Ellefsen', ${row.date_key}, ${JSON.stringify(row.habits)}) ON CONFLICT (missionary_name, date_key) DO NOTHING`;
  }
  const annikaWeekly = await sql`SELECT * FROM weekly_entries WHERE LOWER(missionary_name) = 'annika'`;
  for (const row of annikaWeekly) {
    await sql`INSERT INTO weekly_entries (missionary_name, week_key, challenges) VALUES ('Annika Ellefsen', ${row.week_key}, ${JSON.stringify(row.challenges)}) ON CONFLICT (missionary_name, week_key) DO NOTHING`;
  }
  await sql`DELETE FROM daily_entries  WHERE LOWER(missionary_name) = 'annika'`;
  await sql`DELETE FROM weekly_entries WHERE LOWER(missionary_name) = 'annika'`;
  await sql`DELETE FROM bonus_entries  WHERE LOWER(missionary_name) = 'annika'`;
  await sql`DELETE FROM missionaries   WHERE LOWER(name)            = 'annika'`;

}

exports.handler = async function(event, context) {
  const headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Content-Type": "application/json"
  };

  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers, body: "" };
  }

  const sql = getDb();
  const path = event.path.replace("/.netlify/functions/api", "");
  const method = event.httpMethod;

  try {
    await initDb(sql);

    // GET /names
    if (path === "/names" && method === "GET") {
      const missionaries = await sql`SELECT name FROM missionaries ORDER BY name`;
      return { statusCode: 200, headers, body: JSON.stringify(missionaries.map(m => m.name)) };
    }

    // GET /users
    if (path === "/users" && method === "GET") {
      const missionaries  = await sql`SELECT * FROM missionaries ORDER BY name`;
      const dailyEntries  = await sql`SELECT * FROM daily_entries`;
      const weeklyEntries = await sql`SELECT * FROM weekly_entries`;
      const bonusEntries  = await sql`SELECT * FROM bonus_entries`;
      const result = missionaries.map(m => {
        const days = {};
        dailyEntries.filter(e => e.missionary_name === m.name).forEach(e => { days[e.date_key] = e.habits; });
        const weeks = {};
        weeklyEntries.filter(e => e.missionary_name === m.name).forEach(e => { weeks[e.week_key] = e.challenges; });
        const bonusRow = bonusEntries.find(e => e.missionary_name === m.name);
        return { name: m.name, zone: m.zone, days, weeks, bonus: bonusRow ? bonusRow.bonuses : {} };
      });
      return { statusCode: 200, headers, body: JSON.stringify(result) };
    }

    // GET /user/:name
    if (path.startsWith("/user/") && method === "GET") {
      const name = decodeURIComponent(path.replace("/user/", ""));
      const missionaries = await sql`SELECT * FROM missionaries WHERE LOWER(name) = LOWER(${name})`;
      if (missionaries.length === 0) {
        return { statusCode: 404, headers, body: JSON.stringify({ error: "Not found" }) };
      }
      const m = missionaries[0];
      const dailyEntries  = await sql`SELECT * FROM daily_entries  WHERE missionary_name = ${m.name}`;
      const weeklyEntries = await sql`SELECT * FROM weekly_entries WHERE missionary_name = ${m.name}`;
      const bonusRows     = await sql`SELECT * FROM bonus_entries  WHERE missionary_name = ${m.name}`;
      const days = {};  dailyEntries.forEach(e  => { days[e.date_key]  = e.habits;     });
      const weeks = {}; weeklyEntries.forEach(e => { weeks[e.week_key] = e.challenges; });
      const bonus = bonusRows.length > 0 ? bonusRows[0].bonuses : {};
      return { statusCode: 200, headers, body: JSON.stringify({ name: m.name, zone: m.zone, days, weeks, bonus }) };
    }

    // POST /register
    if (path === "/register" && method === "POST") {
      const body = JSON.parse(event.body);
      const { name, zone } = body;
      if (!name || !zone) return { statusCode: 400, headers, body: JSON.stringify({ error: "Name and zone required" }) };
      const existing = await sql`SELECT * FROM missionaries WHERE LOWER(name) = LOWER(${name})`;
      if (existing.length > 0) return { statusCode: 409, headers, body: JSON.stringify({ error: "Name already taken" }) };
      await sql`INSERT INTO missionaries (name, zone) VALUES (${name}, ${zone})`;
      return { statusCode: 201, headers, body: JSON.stringify({ name, zone, days: {}, weeks: {}, bonus: {} }) };
    }

    // POST /submit
    if (path === "/submit" && method === "POST") {
      const body = JSON.parse(event.body);
      const { name, dateKey, weekKey, habits, challenges, bonus } = body;
      if (!name) return { statusCode: 400, headers, body: JSON.stringify({ error: "Name required" }) };
      if (dateKey && habits) {
        await sql`INSERT INTO daily_entries (missionary_name, date_key, habits) VALUES (${name}, ${dateKey}, ${JSON.stringify(habits)}) ON CONFLICT (missionary_name, date_key) DO UPDATE SET habits = ${JSON.stringify(habits)}, submitted_at = NOW()`;
      }
      if (weekKey && challenges) {
        await sql`INSERT INTO weekly_entries (missionary_name, week_key, challenges) VALUES (${name}, ${weekKey}, ${JSON.stringify(challenges)}) ON CONFLICT (missionary_name, week_key) DO UPDATE SET challenges = ${JSON.stringify(challenges)}, submitted_at = NOW()`;
      }
      if (bonus) {
        await sql`INSERT INTO bonus_entries (missionary_name, bonuses) VALUES (${name}, ${JSON.stringify(bonus)}) ON CONFLICT (missionary_name) DO UPDATE SET bonuses = ${JSON.stringify(bonus)}, submitted_at = NOW()`;
      }
      return { statusCode: 200, headers, body: JSON.stringify({ success: true }) };
    }

    // POST /admin/update-user
    if (path === "/admin/update-user" && method === "POST") {
      const body = JSON.parse(event.body);
      const { originalName, name, zone, days, weeks, bonus } = body;
      if (!originalName) return { statusCode: 400, headers, body: JSON.stringify({ error: "originalName required" }) };

      // Update profile
      await sql`UPDATE missionaries SET name = ${name}, zone = ${zone} WHERE LOWER(name) = LOWER(${originalName})`;

      // Delete ALL existing entries for this user (cleans up duplicate/unpadded keys), then re-insert
      await sql`DELETE FROM daily_entries  WHERE LOWER(missionary_name) = LOWER(${originalName})`;
      await sql`DELETE FROM weekly_entries WHERE LOWER(missionary_name) = LOWER(${originalName})`;
      await sql`DELETE FROM bonus_entries  WHERE LOWER(missionary_name) = LOWER(${originalName})`;

      if (days) {
        for (const [dateKey, habits] of Object.entries(days)) {
          await sql`INSERT INTO daily_entries (missionary_name, date_key, habits) VALUES (${name}, ${dateKey}, ${JSON.stringify(habits)})`;
        }
      }

      if (weeks) {
        for (const [weekKey, challenges] of Object.entries(weeks)) {
          await sql`INSERT INTO weekly_entries (missionary_name, week_key, challenges) VALUES (${name}, ${weekKey}, ${JSON.stringify(challenges)})`;
        }
      }

      if (bonus !== undefined) {
        await sql`INSERT INTO bonus_entries (missionary_name, bonuses) VALUES (${name}, ${JSON.stringify(bonus)})`;
      }

      return { statusCode: 200, headers, body: JSON.stringify({ success: true }) };
    }

    // POST /admin/purge-dates - remove entries for specific date keys across all users
    if (path === "/admin/purge-dates" && method === "POST") {
      const body = JSON.parse(event.body);
      const { dates } = body;
      if (!dates || !Array.isArray(dates)) return { statusCode: 400, headers, body: JSON.stringify({ error: "dates array required" }) };

      let deleted = 0;
      for (const dateKey of dates) {
        const result = await sql`DELETE FROM daily_entries WHERE date_key = ${dateKey}`;
        deleted += result.length || 0;
      }
      return { statusCode: 200, headers, body: JSON.stringify({ success: true, deletedRows: deleted, purgedDates: dates }) };
    }

    // POST /admin/merge-users - merge src into dst, then delete src
    if (path === "/admin/merge-users" && method === "POST") {
      const body = JSON.parse(event.body);
      const { srcName, dstName } = body;
      if (!srcName || !dstName) return { statusCode: 400, headers, body: JSON.stringify({ error: "srcName and dstName required" }) };

      const srcDaily  = await sql`SELECT * FROM daily_entries  WHERE LOWER(missionary_name) = LOWER(${srcName})`;
      const srcWeekly = await sql`SELECT * FROM weekly_entries WHERE LOWER(missionary_name) = LOWER(${srcName})`;
      const srcBonus  = await sql`SELECT * FROM bonus_entries  WHERE LOWER(missionary_name) = LOWER(${srcName})`;

      // Merge daily: dst wins on conflict
      for (const row of srcDaily) {
        await sql`INSERT INTO daily_entries (missionary_name, date_key, habits) VALUES (${dstName}, ${row.date_key}, ${JSON.stringify(row.habits)}) ON CONFLICT (missionary_name, date_key) DO NOTHING`;
      }
      // Merge weekly: dst wins on conflict
      for (const row of srcWeekly) {
        await sql`INSERT INTO weekly_entries (missionary_name, week_key, challenges) VALUES (${dstName}, ${row.week_key}, ${JSON.stringify(row.challenges)}) ON CONFLICT (missionary_name, week_key) DO NOTHING`;
      }
      // Merge bonus: combine keys, dst wins
      if (srcBonus.length > 0) {
        const dstBonus = await sql`SELECT * FROM bonus_entries WHERE LOWER(missionary_name) = LOWER(${dstName})`;
        const merged = Object.assign({}, srcBonus[0].bonuses, dstBonus.length > 0 ? dstBonus[0].bonuses : {});
        await sql`INSERT INTO bonus_entries (missionary_name, bonuses) VALUES (${dstName}, ${JSON.stringify(merged)}) ON CONFLICT (missionary_name) DO UPDATE SET bonuses = ${JSON.stringify(merged)}`;
      }

      // Delete src entirely
      await sql`DELETE FROM daily_entries  WHERE LOWER(missionary_name) = LOWER(${srcName})`;
      await sql`DELETE FROM weekly_entries WHERE LOWER(missionary_name) = LOWER(${srcName})`;
      await sql`DELETE FROM bonus_entries  WHERE LOWER(missionary_name) = LOWER(${srcName})`;
      await sql`DELETE FROM missionaries   WHERE LOWER(name)            = LOWER(${srcName})`;

      return { statusCode: 200, headers, body: JSON.stringify({ success: true }) };
    }

    // POST /admin/delete-user
    if (path === "/admin/delete-user" && method === "POST") {
      const body = JSON.parse(event.body);
      const { name } = body;
      if (!name) return { statusCode: 400, headers, body: JSON.stringify({ error: "name required" }) };
      await sql`DELETE FROM daily_entries  WHERE LOWER(missionary_name) = LOWER(${name})`;
      await sql`DELETE FROM weekly_entries WHERE LOWER(missionary_name) = LOWER(${name})`;
      await sql`DELETE FROM bonus_entries  WHERE LOWER(missionary_name) = LOWER(${name})`;
      await sql`DELETE FROM missionaries   WHERE LOWER(name)            = LOWER(${name})`;
      return { statusCode: 200, headers, body: JSON.stringify({ success: true }) };
    }

    return { statusCode: 404, headers, body: JSON.stringify({ error: "Not found" }) };

  } catch (err) {
    console.error("API error:", err);
    return { statusCode: 500, headers, body: JSON.stringify({ error: err.message }) };
  }
};
