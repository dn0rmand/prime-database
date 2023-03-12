# project-euler-tools
Set of tools for Project Euler

# Installation

With [npm](https://www.npmjs.com/) do:

    $ npm install @dn0rmand/project-euler-tools

# Examples

## BigMap / BigSet
Same as the javascript Map / Set but allows going over the limit of 2^24 entries

```javascript
const { BigMap, BigSet } = require('@dn0rmand/project-euler-tools');

const m = new BigMap();
const s = new BigSet();

m.set('key', 'value');
s.add('key');
```

## binomial
Helper to calculate `n!/p!(n-1)!` with memoization

```javascript
const { binomial } = require('@dn0rmand/project-euler-tools');

console.log(binomial(5, 2)); // n=5 , p=2
console.log(binomial(5n, 2n)); // calculate using BigInt
```

## BitArray
Binary array to store only true(1) or false(0). It uses an `Uint8Array` array so its max size is 8 times the max size of a `Unt8Array`

```javascript
const { BitArray } = require('@dn0rmand/project-euler-tools');

const ar = BitArray(1024);
ar.set(12, true);
ar.get(12);
```

## digits
Returns the digits of a value in a specified base ( defaults to 10 ). Supports both Number and BigInt

```javascript
const { digits } = require('@dn0rmand/project-euler-tools');

const a = digits(12345); // a = [1,2,3,4,5]
const b = digits(0x12345n, 16); // b = [1, 2, 3, 4, 5]
```

## isPrime
Same as [is-number-prime](https://www.npmjs.com/package/is-number-prime) but with a shortcut for numbers lower than 20

## DistinctCollection
Collection of distincts numbers

```javascript
const { DistinctCollection } = require('@dn0rmand/project-euler-tools');

const values = new DistinctCollection();
values.push(1);
values.push(2);
values.push(1);
values.push(3);
values.push(2);

console.log(values.length); // 3
console.log([...values].join(',')); // 1,2,3
```

## divisors
Gets the list of divisors of a number. List is not sorted

```javascript
const { divisors } = require('@dn0rmand/project-euler-tools');

for(const d of divisors(10)) {
  console.log(d);
}

for(const d of divisors(10, myIsPrimeFunction)) {
  console.log(d);
}

divisors(10, undefined, d => {
  console.log(d);
});
```

## divisorsCount
Gets the number of divisors of a number.

```javascript
const { divisorsCount } = require('@dn0rmand/project-euler-tools');

console.log(divisorsCount(10));
```

## fibonacci
Calculates the fibonacci value of a number with modulo, using matrix.

```javascript
const { fibonacci } = require('@dn0rmand/project-euler-tools');

console.log(fibonacci(20000, 1E8));
```

## linearRecurrence
Tries to find a linearRecurrence for a given array of values

## polynomial
Tries to find a polynomial for a given set of values

## primeHelper
Lots of helpers related to prime numbers including mobius and PHI functions.

## primes
Prime generator function. 

```javascript
const { primes } = require('@dn0rmand/project-euler-tools');

const start = 50;
for(const p of primes(start)) {
  if (p > 100) { break; }
  console.log(p);
}
```

## TimeLogger
Allows to calculate how long a process took

```javascript
const { TimeLogger } = require('@dn0rmand/project-euler-tools');

const result = TimeLogger.wrap('processing', _ => doStuff()); 
```
result value will be the value returned by doStuff and the elapsed time will be output to the console

## Tracer
Helper to trace progress while running long calculations. The trace will be output only if a second elapsed since the previous call to `print`
```javascript
const { Tracer } = require('@dn0rmand/project-euler-tools');

const tracer = new Tracer(true);
for (let k = 1; k <= MAX; k++) {
  tracer.print(_ => MAX - k);
  doStuffWithK(k);
}
tracer.clear();
```

## src/numberHelper and src/bigintHelper

Those are automatically included by `require('@dn0rmand/project-euler-tools');`  
They add the following prototypes to Number and BigInt
- `gcd(b)`  
calculates the greatest common divisor of the value and b

- `lcm(b)`  
calculates the least common multiple of the value and b

- `isCoPrime(b)`  
Checks if the value is coprime with b

- `modMul(product, modulo)`  
calculates `(value*product) % modulo`

- `modPow(power, modulo)`  
calculates `(value ** power) % modulo`

- `modInv(modulo)`  
calculates the modulo inverse

- `modDiv(divisor, modulo)`  
calculates `(value * product.modInv(modulo)) % modulo`

The following prototypes are also added to BigInt
- `toExponential(maxDigits)`  
Returns the number converted to a string in the exponential format.
For example the following code with output `1.23457e11`
```javascript
const v = 123456781234n;
console.log(v.toExponential(5));
```  

- `divise(divisor, precision)`  
Divides the `BigInt` value by the divisor (a `BigInt` too) with the required precision and returns the result as a `Number`
