import type {ReactNode} from 'react';
import Head from '@docusaurus/Head';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {useDoc} from '@docusaurus/plugin-content-docs/client';
import {useCanonical} from '@site/src/theme/SiteMetadata/useCanonical';
import {jsonLd, softwareSourceCode} from '@site/src/components/StructuredData/structuredData';

/** The project's structured data, on the page the version's root opens. */
export default function RootStructuredData(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const {metadata} = useDoc();
  const canonical = useCanonical();
  if (metadata.permalink !== siteConfig.baseUrl) {
    return null;
  }
  const url = canonical(`${siteConfig.url}${metadata.permalink}`);
  return (
    <Head>
      <script type="application/ld+json">
        {jsonLd(softwareSourceCode({description: metadata.description, url}))}
      </script>
    </Head>
  );
}
