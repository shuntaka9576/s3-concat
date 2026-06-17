import { Deque } from '../../../lib/std/deque';

describe('Deque', () => {
  test('should correctly handle pushFront and popFront', () => {
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

  test('should correctly handle pushBack and popBack', () => {
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

  test('should correctly return front and back elements', () => {
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

  test('should resize correctly when capacity is full', () => {
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

  test('should handle mixed operations (pushFront, pushBack, popFront, popBack)', () => {
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

  test('iterates non-destructively in front-to-back order via Symbol.iterator', () => {
    const deque = new Deque<number>();
    deque.pushBack(1);
    deque.pushBack(2);
    deque.pushBack(3);

    expect([...deque]).toEqual([1, 2, 3]);
    expect(deque.size).toBe(3);
  });
});
