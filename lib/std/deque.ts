export class Deque<T> {
  private capacity: number;
  private buffer: (T | undefined)[];
  private head: number;
  private tail: number;
  private _size: number;

  constructor(initialCapacity = 8) {
    this.capacity = initialCapacity;
    this.buffer = new Array<T | undefined>(this.capacity);
    this.head = 0;
    this.tail = 0;
    this._size = 0;
  }

  get size(): number {
    return this._size;
  }

  pushFront(item: T): void {
    if (this._size === this.capacity) {
      this.resize();
    }
    this.head = (this.head - 1 + this.capacity) % this.capacity;
    this.buffer[this.head] = item;
    this._size++;
  }

  pushBack(item: T): void {
    if (this._size === this.capacity) {
      this.resize();
    }
    this.buffer[this.tail] = item;
    this.tail = (this.tail + 1) % this.capacity;
    this._size++;
  }

  popFront(): T | undefined {
    if (this._size === 0) {
      return undefined;
    }
    const item = this.buffer[this.head];
    this.buffer[this.head] = undefined;
    this.head = (this.head + 1) % this.capacity;
    this._size--;
    return item;
  }

  popBack(): T | undefined {
    if (this._size === 0) {
      return undefined;
    }
    this.tail = (this.tail - 1 + this.capacity) % this.capacity;
    const item = this.buffer[this.tail];
    this.buffer[this.tail] = undefined;
    this._size--;
    return item;
  }

  front(): T | undefined {
    if (this._size === 0) {
      return undefined;
    }
    return this.buffer[this.head];
  }

  back(): T | undefined {
    if (this._size === 0) {
      return undefined;
    }
    return this.buffer[(this.tail - 1 + this.capacity) % this.capacity];
  }

  *[Symbol.iterator](): Iterator<T> {
    for (let i = 0; i < this._size; i++) {
      yield this.buffer[(this.head + i) % this.capacity] as T;
    }
  }

  private resize(): void {
    const newCapacity = this.capacity * 2;
    const newBuffer = new Array<T | undefined>(newCapacity);

    for (let i = 0; i < this._size; i++) {
      newBuffer[i] = this.buffer[(this.head + i) % this.capacity];
    }

    this.buffer = newBuffer;
    this.head = 0;
    this.tail = this._size;
    this.capacity = newCapacity;
  }
}
