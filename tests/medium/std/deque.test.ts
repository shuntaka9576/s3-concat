import { Deque } from '../../../lib/std/deque';

describe('Deque', () => {
  it('should correctly handle pushFront and popFront', () => {
    const deque = new Deque<number>();

    expect(deque.size).toBe(0);

    deque.pushFront(1);
    expect(deque.size).toBe(1);
    deque.pushFront(2);
    expect(deque.size).toBe(2);

    expect(deque.popFront()).toBe(2);
    expect(deque.popFront()).toBe(1);
    expect(deque.popFront()).toBeUndefined();
    expect(deque.size).toBe(0);
  });

  it('should correctly handle pushBack and popBack', () => {
    const deque = new Deque<number>();

    expect(deque.size).toBe(0);

    deque.pushBack(1);
    expect(deque.size).toBe(1);
    deque.pushBack(2);
    expect(deque.size).toBe(2);

    expect(deque.popBack()).toBe(2);
    expect(deque.popBack()).toBe(1);
    expect(deque.popBack()).toBeUndefined();
    expect(deque.size).toBe(0);
  });

  it('should correctly return front and back elements', () => {
    const deque = new Deque<number>();

    expect(deque.front()).toBeUndefined();
    expect(deque.back()).toBeUndefined();

    deque.pushFront(1);
    deque.pushBack(2);
    expect(deque.front()).toBe(1);
    expect(deque.back()).toBe(2);

    deque.pushFront(3);
    deque.pushBack(4);
    expect(deque.front()).toBe(3);
    expect(deque.back()).toBe(4);
  });

  it('should resize correctly when capacity is full', () => {
    const deque = new Deque<number>(2);

    deque.pushBack(1);
    deque.pushBack(2);

    deque.pushBack(3);
    expect(deque.size).toBe(3);
    expect(deque.front()).toBe(1);
    expect(deque.back()).toBe(3);

    expect(deque.popFront()).toBe(1);
    expect(deque.popFront()).toBe(2);
    expect(deque.popFront()).toBe(3);
  });

  it('should handle mixed operations (pushFront, pushBack, popFront, popBack)', () => {
    const deque = new Deque<number>();

    deque.pushBack(1);
    deque.pushFront(2);
    deque.pushFront(3);
    deque.pushBack(4);

    expect(deque.size).toBe(4);
    expect(deque.front()).toBe(3);
    expect(deque.back()).toBe(4);

    expect(deque.popFront()).toBe(3);
    expect(deque.popBack()).toBe(4);

    expect(deque.size).toBe(2);
    expect(deque.front()).toBe(2);
    expect(deque.back()).toBe(1);
  });
});
