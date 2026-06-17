const storageUnits = ['B', 'KiB', 'MiB', 'GiB', 'TiB'] as const;
export type StorageUnit = (typeof storageUnits)[number];
export type StorageSize<U extends StorageUnit> = `${number}${U}`;

export const KiB = 1024;
export const MiB = 1024 * KiB;
export const GiB = 1024 * MiB;
export const TiB = 1024 * GiB;

const STORAGE_SIZE_REGEX = new RegExp(`^\\d+(${storageUnits.join('|')})$`);

const STORAGE_UNITS_LONGEST_FIRST = [...storageUnits].sort(
  (a, b) => b.length - a.length
);

const isStorageSize = (input: string): input is StorageSize<StorageUnit> =>
  STORAGE_SIZE_REGEX.test(input);

const parseStorageSize = <U extends StorageUnit>(
  input: string
): [number, U] | undefined => {
  if (!isStorageSize(input)) {
    return undefined;
  }
  const unit = STORAGE_UNITS_LONGEST_FIRST.find((u) => input.endsWith(u)) as U;
  const numberPart = input.slice(0, -unit.length);
  const value = Number.parseInt(numberPart, 10);

  return [value, unit];
};

export const sizeToBytes = <U extends StorageUnit>(
  size: StorageSize<U>
): number => {
  const result = parseStorageSize(size);
  if (result === undefined) {
    throw new Error('parse error');
  }

  const [value, unit] = result;

  switch (unit) {
    case 'B':
      return value;
    case 'KiB':
      return value * KiB;
    case 'MiB':
      return value * MiB;
    case 'GiB':
      return value * GiB;
    case 'TiB':
      return value * TiB;
    default: {
      const exhaustiveCheck: never = unit;

      throw new Error(`Unhandled unit: ${exhaustiveCheck}`);
    }
  }
};
