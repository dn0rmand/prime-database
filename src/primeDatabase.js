const fs = require('fs');
const AdmZip = require('adm-zip');
const Database = require('better-sqlite3');

let $db = undefined;
let $dbCache = {};

function resetDB() {
  $dbCache = {};
}

function openDB() {
  if ($db) {
    return $db;
  }

  const dbFileName = `${__dirname}/primeCounts.sqlite`;
  const dbZipName = dbFileName + '.zip';

  if (!fs.existsSync(dbFileName) && fs.existsSync(dbZipName)) {
    const zip = new AdmZip(dbZipName);
    zip.extractAllTo(__dirname, false);
    if (!fs.existsSync(dbFileName)) {
      throw `Error: Failed to unzip ${dbZipName}`;
    }
  }

  $db = new Database(dbFileName);
  $db.exec(`
    CREATE TABLE IF NOT EXISTS "prime_counts" 
    (
        "value" INTEGER PRIMARY KEY,
        "primes" INTEGER NOT NULL
    )
    `);

  return $db;
}

function getPrimeGroups(start, callback) {
  const db = openDB();
  const stm = db.prepare(`
        SELECT 
            max(value) value, primes count 
        FROM 
            prime_counts 
        WHERE 
            value >= ? 
        GROUP BY 
            primes order by value
    `);

  for (let row of stm.iterate(+start)) {
    if (callback(row.value, row.count) === false) {
      break;
    }
  }
}

function getPrimeCount(num) {
  if ($dbCache[num] !== undefined) {
    return $dbCache[num];
  }

  const db = openDB();
  let count = db.prepare('SELECT primes FROM prime_counts WHERE "value"=?').get(num);
  if (count !== undefined) {
    count = +count.primes;
    $dbCache[num] = count;
    return count;
  }
}

function setPrimeCount(num, value) {
  if (getPrimeCount(num) === undefined) {
    const db = openDB();
    db.prepare('REPLACE INTO prime_counts ("value", "primes") VALUES (?, ?)').run(num, value);

    $dbCache[num] = value;
    return true;
  } else {
    return false;
  }
}

function savePrimeCounts(counts, num, trace) {
  if (trace) {
    process.stdout.write(`\rSaving results .....`);
  }

  let count = s[num];
  let added = 0;
  openDB().transaction(() => {
    for (let i in counts) {
      i = +i;
      if (setPrimeCount(i, counts[i])) {
        added++;
      }
    }

    if (setPrimeCount(num, count)) {
      added++;
    }
  })();

  if (trace) {
    console.log(`\r${added} values added to database          `);
  }

  return count;
}

module.exports = {
  resetDB,
  getPrimeGroups,
  getPrimeCount,
  setPrimeCount,
  savePrimeCounts,
};
