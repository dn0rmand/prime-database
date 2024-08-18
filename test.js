const { getPrimeCount, createZippedDB } = require('./src/primeDatabase');

// createZippedDB();
const count = getPrimeCount(1e8);

console.log(`${count} primes up to 1e8`);
