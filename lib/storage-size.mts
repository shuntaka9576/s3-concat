const storageUnits = ['B', 'KiB', 'MiB', 'GiB', 'TiB'] as const;
export type StorageUnit = (typeof storageUnits)[number];
export type StorageSize<U extends StorageUnit> = `${number}${U}`;

const KiB = 1024;
const MiB = 1024 * KiB;
const GiB = 1024 * MiB;
const TiB = 1024 * GiB;

const isStorageSize = (input: string): input is StorageSize<StorageUnit> => {
  const regex = new RegExp(`^\\d+(${storageUnits.join('|')})$`);

  return regex.test(input);
};

const parseStorageSize = <U extends StorageUnit>(
  input: string
): [number, U] | null => {
  if (isStorageSize(input)) {
    const sortedUnits = storageUnits
      .slice()
      .sort((a, b) => b.length - a.length);
    const unit = sortedUnits.find((u) => input.endsWith(u)) as U;
    const numberPart = input.slice(0, -unit.length);
    const value = Number.parseInt(numberPart, 10);

    return [value, unit];
  }

  return null;
};

export const sizeToBytes = <U extends StorageUnit>(
  size: StorageSize<U>
): number => {
  const result = parseStorageSize(size);
  if (result == null) {
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
