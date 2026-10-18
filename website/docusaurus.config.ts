import {existsSync, readFileSync} from 'node:fs';
import path from 'node:path';
import {themes as prismThemes} from 'prism-react-renderer';
import type {Config, Plugin} from '@docusaurus/types';
import {getVcsPreset} from '@docusaurus/utils';
import type * as Preset from '@docusaurus/preset-classic';
import {resolveMarkdownLinks} from './markdownLinks';
import remarkApiLinks from './plugins/remarkApiLinks';
import remarkSnippets from './plugins/remarkSnippets';
import publishFilesPlugin from './plugins/publishFiles';
import redirectsPlugin from './plugins/redirects';
import {siteRelativeVcs} from './plugins/siteRelativeVcs';
import {redirects as guideRedirects} from './redirects';
import {searchLanguages} from './search';
import {translatableLabels} from './sidebarItems';
import {coreLanding, readBuildVersion, siteRoot} from './versioning';

// The look of every RustStream site: the core site publishes its palette, fonts, logos and favicon
// at one address outside its versions, and each broker site loads them from there. The theme's
// stylesheet comes before the bundled one, and still wins: the bundle puts Infima and the
// Docusaurus themes into cascade layers (`future.v4`), which a rule outside any layer overrides.
const sharedTheme = 'https://powersemmi.github.io/ruststream/theme/v1/';

// Every minor line is built on its own and served from `/ruststream-rumqttc/<version>/`, with a
// second build of the newest line under `/ruststream-rumqttc/latest/` and a build of `main` under
// `/ruststream-rumqttc/next/`. The deploy script sets the base URL and the kind of version per build, so
// one configuration serves every version directory. An old line is built from its archive
// checkout, with the site code of this one.
const repoRoot = path.resolve(__dirname, '..');
const version = readBuildVersion(process.env, repoRoot);
const {baseUrl, contentRoot, archive} = version;
const docsRoot = path.join(contentRoot, 'docs');

// Excerpts on the pages are read from the crate's compiled sources and link back to them; a page
// names a source by its path in the crate (`examples/<file>.rs`).
const crateDir = 'crates/ruststream-rumqttc';
const snippetRoot = path.join(contentRoot, crateDir);
const sourceUrl = `https://github.com/powersemmi/ruststream-rumqttc/blob/${version.sourceRef}/${crateDir}`;
const editBase = 'https://github.com/powersemmi/ruststream-rumqttc/edit/main';

// A translated page is edited in its own file; a page the locale has not translated yet is
// served from the English text, so its edit link opens the English file.
function editUrl({locale, docPath}: {locale: string; docPath: string}): string {
  const localized = `website/i18n/${locale}/docusaurus-plugin-content-docs/current/${docPath}`;
  if (locale !== 'en' && existsSync(path.join(repoRoot, localized))) {
    return `${editBase}/${localized}`;
  }
  return `${editBase}/docs/${docPath}`;
}

// An archive is the line as it was released, in English; its pages carry the sidebar and the
// moved addresses of that line. The current line keeps its translations and the addresses of the
// earlier site.
const locales = archive ? ['en'] : ['en', 'ru', 'zh'];
const sidebarPath = archive ? path.join(contentRoot, 'sidebars.json') : './sidebars.ts';
const archiveRedirects = path.join(contentRoot, 'redirects.json');
const redirects: Record<string, string> = archive
  ? existsSync(archiveRedirects)
    ? JSON.parse(readFileSync(archiveRedirects, 'utf8'))
    : {}
  : guideRedirects(version.docsRs);

// The language a build renders: Docusaurus loads this file once per language and names it here.
const buildLocale = process.env.DOCUSAURUS_CURRENT_LOCALE ?? 'en';

// API links on the pages resolve against the rustdoc of the checkout being built, which is the
// rustdoc docs.rs renders for that version of this crate and of the core it depends on
// (`scripts/site_rustdoc.sh`).
const rustdocDir = process.env.DOCS_RUSTDOC_DIR ?? path.join(contentRoot, 'target', 'doc');

// The navbar opens each category of the documentation. The API is the crate's rustdoc of the
// version built; an archive keeps the one sidebar of its line.
const api = {href: `${version.docsRs}ruststream_rumqttc/`, label: 'API', position: 'left'} as const;
const sidebar = (sidebarId: string, label: string) =>
  ({type: 'docSidebar', sidebarId, label, position: 'left'}) as const;
const categories = archive
  ? [sidebar('docs', 'Docs'), api]
  : [
      sidebar('guides', 'Guides'),
      sidebar('components', 'Components'),
      api,
      sidebar('architecture', 'Architecture'),
    ];

// The search plugin offers an "Ask AI" entry when `askAi` is set and renders the module
// `open-ask-ai` as the widget that answers it. Pointing that module at our adapter sends the
// entry to the site's own panel instead of the third-party widget.
function askAiInSearch(): Plugin {
  return {
    name: 'ruststream-ask-ai-in-search',
    configureWebpack() {
      return {
        resolve: {
          alias: {
            'open-ask-ai$': path.resolve(__dirname, 'src/components/AskAi/searchAdapter.tsx'),
            'open-ask-ai/styles.css': path.resolve(__dirname, 'src/components/AskAi/empty.css'),
          },
        },
      };
    },
  };
}

const config: Config = {
  title: 'RustStream MQTT',
  tagline: 'The MQTT 5 broker for RustStream',
  favicon: `${sharedTheme}img/favicon.svg`,
  stylesheets: [`${sharedTheme}theme.css`],

  future: {
    v4: true,
    faster: true,
    // The eager Git reader that `faster` selects, made to answer the sitemap's `lastmod` lookups.
    experimental_vcs: siteRelativeVcs(getVcsPreset('default-v2')),
  },

  // One namespace for every language and version build, so a reader's colour theme and tab
  // choices follow them from /next/ru/ to /latest/. The v4 default hashes url + baseUrl, which
  // differs per build; a fixed name still keeps the keys apart from the other sites on
  // powersemmi.github.io, the core's included.
  storage: {
    type: 'localStorage',
    namespace: 'ruststream-rumqttc',
  },

  url: 'https://powersemmi.github.io',
  baseUrl,
  trailingSlash: true,

  organizationName: 'powersemmi',
  projectName: 'ruststream-rumqttc',

  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',
  onDuplicateRoutes: 'throw',

  customFields: {
    siteRoot,
    docsRs: version.docsRs,
    sourceUrl,
    // The canonical URLs and language alternates of every page name this copy of the site. A
    // language's build sees its own base URL, so the version's one travels beside it.
    buildBaseUrl: version.baseUrl,
    canonicalBaseUrl: version.canonicalBaseUrl,
  },

  // `next` documents unreleased code and an archive an old release; search engines should send
  // readers to `latest`. The tag goes on every page and still lets a crawler follow the pages' links to `latest`, which the site option `noIndex` would forbid.
  headTags: version.noIndex
    ? [{tagName: 'meta', attributes: {name: 'robots', content: 'noindex'}}]
    : [],

  i18n: {
    defaultLocale: 'en',
    locales,
    // Each language is built in a process of its own (`build.mjs`), and Docusaurus leaves the
    // language segment out of the base URL of a build that names a single language; the segment
    // is therefore written here.
    localeConfigs: {
      en: {label: 'English', htmlLang: 'en', baseUrl},
      ru: {label: 'Русский', htmlLang: 'ru', path: 'ru', baseUrl: `${baseUrl}ru/`},
      zh: {label: '中文', htmlLang: 'zh-CN', path: 'zh', baseUrl: `${baseUrl}zh/`},
    },
  },

  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: resolveMarkdownLinks(),
    },
  },

  themes: [
    '@docusaurus/theme-mermaid',
    [
      '@easyops-cn/docusaurus-search-local',
      {
        hashed: true,
        language: searchLanguages(buildLocale),
        indexBlog: false,
        docsDir: docsRoot,
        docsRouteBasePath: '/',
        highlightSearchTermsOnTargetPage: true,
        // `next` and the archives ask search engines to leave their pages out; the site's own
        // search still indexes them, or a reader of those versions would find nothing.
        forceIgnoreNoIndex: true,
        askAi: {},
      },
    ],
  ],

  plugins: [
    askAiInSearch,
    (context) => redirectsPlugin(context, {redirects}),
    (context) =>
      publishFilesPlugin(context, {
        rootDir: contentRoot,
        files: {'benchmarks/results.json': 'docs/benchmarks/results.json'},
      }),
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          path: docsRoot,
          routeBasePath: '/',
          sidebarPath,
          // Components follow their directory tree; their labels are translated like the rest.
          sidebarItemsGenerator: async ({defaultSidebarItemsGenerator, ...args}) =>
            translatableLabels(await defaultSidebarItemsGenerator(args), {
              'components/index': 'Overview',
            }),
          // Before the default plugins, which would otherwise read an `api:` link as a file.
          beforeDefaultRemarkPlugins: [[remarkApiLinks, {docsRs: version.apiDocs, rustdocDir}]],
          remarkPlugins: [[remarkSnippets, {rootDir: snippetRoot, repoUrl: sourceUrl}]],
          // An archive is not edited: its fixes land in `main`.
          editUrl: archive ? undefined : editUrl,
          // The build holds one version; its kind decides the banner on every page.
          versions: {current: version.docsVersion},
          // The benchmark results and the excerpts a test compares against are data, not pages.
          exclude: ['benchmarks/**', 'snippets/**'],
        },
        blog: false,
        // Every version opens on its first page of documentation: the Guides category, or an
        // archive's first page.
        pages: false,
        // Only the indexed copy lists its pages. Google ignores `changefreq` and `priority`;
        // `lastmod` comes from the git history of each page. The search page holds nothing
        // without a query.
        sitemap: version.sitemap
          ? {
              lastmod: 'date',
              changefreq: null,
              priority: null,
              ignorePatterns: locales.map((locale) =>
                locale === 'en' ? `${baseUrl}search/**` : `${baseUrl}${locale}/search/**`,
              ),
            }
          : false,
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    // The card link previews show: on every page, as `og:image` and `twitter:image`.
    image: 'img/social-card.png',
    metadata: [
      {property: 'og:image:width', content: '1200'},
      {property: 'og:image:height', content: '630'},
      {property: 'og:image:alt', content: 'RustStream MQTT: the MQTT 5 broker for RustStream'},
    ],
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: false,
      disableSwitch: false,
    },
    navbar: {
      title: 'RustStream',
      // The brand leads to the framework's own site, in the reader's language and the same tab;
      // this site's root is the Guides category in the navbar.
      logo: {
        alt: 'RustStream',
        src: `${sharedTheme}img/logo.svg`,
        srcDark: `${sharedTheme}img/logo-dark.svg`,
        href: coreLanding(buildLocale),
        target: '_self',
      },
      items: [
        ...categories,
        {type: 'custom-versionDropdown', position: 'left'},
        {type: 'custom-askAi', position: 'right'},
        // An archive is built in English only; a language menu would offer nothing to switch to.
        ...(archive ? [] : [{type: 'localeDropdown' as const, position: 'right' as const}]),
        {
          href: 'https://github.com/powersemmi/ruststream-rumqttc',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['toml', 'bash'],
    },
    mermaid: {
      theme: {light: 'neutral', dark: 'dark'},
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
