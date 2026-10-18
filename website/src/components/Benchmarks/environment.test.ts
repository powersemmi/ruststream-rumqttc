import {describe, expect, it} from 'vitest';
import {environmentRows, type EnvironmentLabels} from './environment';

const words: EnvironmentLabels = {
  cpu: 'Processor',
  round_trip: 'Round trip',
};

describe('the machine the numbers were taken on', () => {
  it('lists every field in its order and as recorded', () => {
    const rows = environmentRows({cpu: 'x86', round_trip: '61 µs', rustc: '1.98.1'}, words);
    expect(rows.map((row) => row.field)).toEqual(['cpu', 'round_trip', 'rustc']);
    expect(rows.map((row) => row.value)).toEqual(['x86', '61 µs', '1.98.1']);
  });

  it('names a field in the words of the page', () => {
    const rows = environmentRows({cpu: 'x86', round_trip: '77 µs'}, words);
    expect(rows.map((row) => row.label)).toEqual(['Processor', 'Round trip']);
  });

  it('names a field the page has no word for by its key, so a new field is never dropped', () => {
    const rows = environmentRows({kernel_tuning: 'none'}, words);
    expect(rows).toEqual([{field: 'kernel_tuning', label: 'kernel_tuning', value: 'none'}]);
  });

  it('leaves out a field the document left empty', () => {
    expect(environmentRows({cpu: 'x86', memory_speed: undefined}, words)).toHaveLength(1);
  });
});
