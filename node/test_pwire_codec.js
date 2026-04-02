/**
 * Unit tests for the pure-JS PWire codec with LZ4 compression.
 */

'use strict';

const pwire = require('./pwire');

let totalTests = 0;
let passedTests = 0;
let failedTests = 0;
const failures = [];

function assert(condition, message) {
    if (!condition) throw new Error(`Assertion failed: ${message}`);
}

function assertEqual(actual, expected, message) {
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
        throw new Error(
            `${message}\n  Expected: ${JSON.stringify(expected)}\n  Actual:   ${JSON.stringify(actual)}`
        );
    }
}

function test(name, fn) {
    totalTests++;
    try {
        fn();
        passedTests++;
        console.log(`  PASS  ${name}`);
    } catch (e) {
        failedTests++;
        failures.push({ name, error: e.message });
        console.log(`  FAIL  ${name}`);
        console.log(`        ${e.message.split('\n')[0]}`);
    }
}

console.log('\n=== PWire Codec Unit Tests ===\n');

// ── Frame encoding ──────────────────────────────────────────────────────

test('frame builds correct header', () => {
    const payload = Buffer.from('hello', 'utf8');
    const f = pwire.frame(0x01, payload);
    assertEqual(f.readUInt8(0), 0x01, 'type byte');
    assertEqual(f.readUInt32LE(1), 5, 'payload length');
    assertEqual(f.slice(5).toString('utf8'), 'hello', 'payload content');
});

test('encodeAuth builds correct frame', () => {
    const f = pwire.encodeAuth('admin', 'secret');
    assertEqual(f.readUInt8(0), pwire.MSG_AUTH, 'type');
});

test('encodeAuthWithCaps includes caps byte', () => {
    const f = pwire.encodeAuthWithCaps('admin', 'secret', pwire.CAP_LZ4);
    assertEqual(f.readUInt8(0), pwire.MSG_AUTH, 'type');
    const payloadLen = f.readUInt32LE(1);
    // user(1+5) + pass(1+6) + caps(1) = 14
    assertEqual(payloadLen, 14, 'payload length');
    assertEqual(f.readUInt8(pwire.HEADER_SIZE + 13), pwire.CAP_LZ4, 'caps byte');
});

test('encodePrepare builds correct frame', () => {
    const f = pwire.encodePrepare('SELECT $1');
    assertEqual(f.readUInt8(0), pwire.MSG_PREPARE, 'type');
    const payload = f.slice(pwire.HEADER_SIZE);
    assertEqual(payload.toString('utf8'), 'SELECT $1', 'SQL');
});

test('encodeExecute builds correct frame', () => {
    const f = pwire.encodeExecute(42, ['hello', 'world']);
    assertEqual(f.readUInt8(0), pwire.MSG_EXECUTE, 'type');
    const payload = f.slice(pwire.HEADER_SIZE);
    assertEqual(payload.readUInt32LE(0), 42, 'handle');
    assertEqual(payload.readUInt16LE(4), 2, 'param count');
});

test('encodeClose builds correct frame', () => {
    const f = pwire.encodeClose(99);
    assertEqual(f.readUInt8(0), pwire.MSG_CLOSE, 'type');
    const payload = f.slice(pwire.HEADER_SIZE);
    assertEqual(payload.readUInt32LE(0), 99, 'handle');
});

// ── LZ4 compression ─────────────────────────────────────────────────────

let hasLZ4 = false;
try { require('lz4'); hasLZ4 = true; } catch (_) {}

test('compressFrame does not compress small payloads', () => {
    const small = Buffer.alloc(100, 0x41);
    const f = pwire.compressFrame(pwire.MSG_QUERY, small);
    assertEqual(f.readUInt8(0), pwire.MSG_QUERY, 'should not be compressed');
});

if (hasLZ4) {
    test('compressFrame compresses large compressible payloads', () => {
        const large = Buffer.alloc(16 * 1024, 0x58);
        const f = pwire.compressFrame(pwire.MSG_QUERY, large);
        assertEqual(f.readUInt8(0), pwire.MSG_COMPRESSED, 'should be MSG_COMPRESSED');
        assert(f.length < large.length, 'compressed should be smaller');
    });

    test('compress/decompress round-trip', () => {
        const original = Buffer.alloc(16 * 1024);
        for (let i = 0; i < original.length; i++) original[i] = i % 26 + 97;
        const f = pwire.compressFrame(pwire.MSG_QUERY, original);
        assertEqual(f.readUInt8(0), pwire.MSG_COMPRESSED, 'should be compressed');

        const payloadLen = f.readUInt32LE(1);
        const innerPayload = f.slice(pwire.HEADER_SIZE, pwire.HEADER_SIZE + payloadLen);

        const result = pwire.decompressFrame(innerPayload);
        assertEqual(result.type, pwire.MSG_QUERY, 'original type');
        assert(result.payload.equals(original), 'decompressed should match original');
    });
} else {
    console.log('  SKIP  LZ4 tests (lz4 package not installed)');
}

// ── Response decoding ───────────────────────────────────────────────────

test('decodeResultSet decodes a simple result set', () => {
    const parts = [];
    // col count = 1
    const colCountBuf = Buffer.alloc(2);
    colCountBuf.writeUInt16LE(1, 0);
    parts.push(colCountBuf);
    // column: name "name", type TEXT
    parts.push(Buffer.from([4])); // name length
    parts.push(Buffer.from('name', 'utf8'));
    parts.push(Buffer.from([pwire.TYPE_TEXT]));
    // row count = 1
    const rowCountBuf = Buffer.alloc(4);
    rowCountBuf.writeUInt32LE(1, 0);
    parts.push(rowCountBuf);
    // null bitmap (1 byte, no nulls)
    parts.push(Buffer.from([0]));
    // text value: "alice"
    const lenBuf = Buffer.alloc(2);
    lenBuf.writeUInt16LE(5, 0);
    parts.push(lenBuf);
    parts.push(Buffer.from('alice', 'utf8'));

    const payload = Buffer.concat(parts);
    const result = pwire.decodeResultSet(payload);
    assertEqual(result.columns, ['name'], 'columns');
    assertEqual(result.rows.length, 1, 'row count');
    assertEqual(result.rows[0][0], 'alice', 'value');
});

test('decodeOk decodes an OK response', () => {
    const payload = Buffer.alloc(9 + 5);
    // rows affected = 42
    payload.writeUInt32LE(42, 0);
    payload.writeInt32LE(0, 4);
    // tag = "DONE!"
    payload.writeUInt8(5, 8);
    Buffer.from('DONE!').copy(payload, 9);
    const result = pwire.decodeOk(payload);
    assertEqual(result.rows_affected, 42, 'rows affected');
    assertEqual(result.tag, 'DONE!', 'tag');
});

test('decodeError decodes an ERROR response', () => {
    const parts = [];
    parts.push(Buffer.from('42P01', 'ascii'));
    const lenBuf = Buffer.alloc(2);
    lenBuf.writeUInt16LE(13, 0);
    parts.push(lenBuf);
    parts.push(Buffer.from('table missing', 'utf8'));
    const payload = Buffer.concat(parts);
    const result = pwire.decodeError(payload);
    assertEqual(result.sqlState, '42P01', 'sqlState');
    assertEqual(result.message, 'table missing', 'message');
});

// ── Summary ─────────────────────────────────────────────────────────────

console.log('\n' + '='.repeat(50));
console.log(`  Results: ${passedTests} passed, ${failedTests} failed, ${totalTests} total`);
console.log('='.repeat(50));

if (failures.length > 0) {
    console.log('\nFailures:');
    for (const f of failures) {
        console.log(`  - ${f.name}: ${f.error.split('\n')[0]}`);
    }
}

console.log('');
process.exit(failedTests > 0 ? 1 : 0);
