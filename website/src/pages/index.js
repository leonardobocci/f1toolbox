import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';

import Heading from '@theme/Heading';
import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="docs/category/f1toolbox---overview">
            Check out how I built it!
          </Link>
        </div>
        <div>
          <iframe
            src="https://metabase.f1toolbox.com/public/dashboard/e99ff64b-d510-4305-be84-ae0198682c6f"
            width="100%"
            height="100%"
            allowTransparency
            style={{ minHeight: '820px' }}
          ></iframe>
        </div>
        <div>
          <iframe
            src="https://metabase.f1toolbox.com/public/dashboard/074743a3-6762-4bb4-883a-8de7043ab5fc"
            width="100%"
            height="100%"
            allowTransparency
            style={{ minHeight: '820px' }}
          ></iframe>
        </div>
        <div>
          <Link
              className="button button--secondary button--lg"
              href="https://buymeacoffee.com/f1toolbox">
              Liked it? Buy me a coffee!
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={`${siteConfig.title}`}
      description="Description will go into a meta tag in <head />">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
