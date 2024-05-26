import {
  type StorageSize,
  type StorageUnit,
  sizeToBytes,
} from '../../lib/storage-size.mjs';
import { GiB, KiB, MiB, TiB } from './../helpers/value.mjs';

describe('sizeToBytes', () => {
  test.each<{ value: StorageSize<StorageUnit>; expected: number }>([
    { value: '5B', expected: 5 },
    { value: '5KiB', expected: 5 * KiB },
    { value: '5MiB', expected: 5 * MiB },
    { value: '5GiB', expected: 5 * GiB },
    { value: '5TiB', expected: 5 * TiB },
  ])('ReturnBytes', ({ value, expected }) => {
    // When:
    const got = sizeToBytes(value);

    // Then:
    expect(got).toEqual(expected);
  });
});
