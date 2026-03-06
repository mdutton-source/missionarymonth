const { neon } = require("@neondatabase/serverless");

function getDb() {
  return neon(process.env.NETLIFY_DATABASE_URL);
}

async function initDb(sql) {
  await sql`CREATE TABLE IF NOT EXISTS missionaries (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE, zone TEXT NOT NULL, created_at TIMESTAMP DEFAULT NOW())`;
  await sql`CREATE TABLE IF NOT EXISTS daily_entries (id SERIAL PRIMARY KEY, missionary_name TEXT NOT NULL, date_key TEXT NOT NULL, habits JSONB NOT NULL DEFAULT '{}', submitted_at TIMESTAMP DEFAULT NOW(), UNIQUE(missionary_name, date_key))`;
  await sql`CREATE TABLE IF NOT EXISTS weekly_entries (id SERIAL PRIMARY KEY, missionary_name TEXT NOT NULL, week_key TEXT NOT NULL, challenges JSONB NOT NULL DEFAULT '{}', submitted_at TIMESTAMP DEFAULT NOW(), UNIQUE(missionary_name, week_key))`;
  await sql`CREATE TABLE IF NOT EXISTS bonus_entries (id SERIAL PRIMARY KEY, missionary_name TEXT NOT NULL UNIQUE, bonuses JSONB NOT NULL DEFAULT '{}', submitted_at TIMESTAMP DEFAULT NOW())`;
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

      await sql`UPDATE missionaries SET name = ${name}, zone = ${zone} WHERE LOWER(name) = LOWER(${originalName})`;

      if (days) {
        for (const [dateKey, habits] of Object.entries(days)) {
          await sql`INSERT INTO daily_entries (missionary_name, date_key, habits) VALUES (${name}, ${dateKey}, ${JSON.stringify(habits)}) ON CONFLICT (missionary_name, date_key) DO UPDATE SET habits = ${JSON.stringify(habits)}, submitted_at = NOW()`;
        }
        if (name !== originalName) {
          await sql`UPDATE daily_entries SET missionary_name = ${name} WHERE LOWER(missionary_name) = LOWER(${originalName})`;
        }
      }

      if (weeks) {
        for (const [weekKey, challenges] of Object.entries(weeks)) {
          await sql`INSERT INTO weekly_entries (missionary_name, week_key, challenges) VALUES (${name}, ${weekKey}, ${JSON.stringify(challenges)}) ON CONFLICT (missionary_name, week_key) DO UPDATE SET challenges = ${JSON.stringify(challenges)}, submitted_at = NOW()`;
        }
        if (name !== originalName) {
          await sql`UPDATE weekly_entries SET missionary_name = ${name} WHERE LOWER(missionary_name) = LOWER(${originalName})`;
        }
      }

      if (bonus !== undefined) {
        await sql`INSERT INTO bonus_entries (missionary_name, bonuses) VALUES (${name}, ${JSON.stringify(bonus)}) ON CONFLICT (missionary_name) DO UPDATE SET bonuses = ${JSON.stringify(bonus)}, submitted_at = NOW()`;
        if (name !== originalName) {
          await sql`UPDATE bonus_entries SET missionary_name = ${name} WHERE LOWER(missionary_name) = LOWER(${originalName})`;
        }
      }

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
