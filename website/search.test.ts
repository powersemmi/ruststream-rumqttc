import {createRequire} from 'node:module';
import {describe, expect, it} from 'vitest';
import {searchLanguages} from './search';

const require = createRequire(import.meta.url);

// The search plugin's own index builder, as its post-build step runs it for one language build.
const {buildIndex} = require('@easyops-cn/docusaurus-search-local/dist/server/server/utils/buildIndex') as {
  buildIndex: (
    documents: {i: number; t: string}[][],
    options: {language: string[]; removeDefaultStopWordFilter: string[]; removeDefaultStemmer: boolean},
  ) => {index: {search: (query: string) => unknown[]}}[];
};

describe('searchLanguages', () => {
  it('indexes the English build in English only', () => {
    expect(searchLanguages('en')).toEqual(['en']);
  });

  it('indexes a translated build in its language beside English, which names the API', () => {
    expect(searchLanguages('ru')).toEqual(['en', 'ru']);
    expect(searchLanguages('zh')).toEqual(['en', 'zh']);
  });

  it('finds a Russian word and an English identifier in the Russian build', () => {
    const [{index}] = buildIndex(
      [[{i: 1, t: 'Подписчик подтверждает сообщение темы через MqttBroker'}]],
      {language: searchLanguages('ru'), removeDefaultStopWordFilter: [], removeDefaultStemmer: false},
    );
    expect(index.search('сообщение')).toHaveLength(1);
    expect(index.search('mqttbroker')).toHaveLength(1);
  });
});
