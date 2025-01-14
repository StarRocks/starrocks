import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';
import React from 'react';
import Link from '@docusaurus/Link';

const EnglishFeatureList = [
  {
    title: 'Introduction',
    url: '../../introduction/',
    description: (
      <>
        OLAP, features, architecture
      </>
    ),
  },
  {
    title: 'Quick Start',
    url: '../../quick_start/basic',
    description: (
      <>
        Get up and running quickly.
      </>
    ),
  },
  {
    title: 'Data Loading',
    url: '../../loading/loading_introduction/Loading_intro/',
    description: (
      <>
        Clean, transform, and load
      </>
    ),
  },
  {
    title: 'Table Design',
    url: '../../table_design/StarRocks_table_design/',
    description: (
      <>
        Tables, indexing, acceleration
      </>
    ),
  },
  {
    title: 'Data Lakes',
    url: '../../data_source/data_lakes/',
    description: (
      <>
        Iceberg, Hive, Delta Lake, …
      </>
    ),
  },
  {
    title: 'Work with semi-structured data',
    url: '../../sql-reference/data-types/semi_structured/',
    description: (
      <>
        JSON, map, struct, array
      </>
    ),
  },
  {
    title: 'Integrations',
    url: '../../integrations/',
    description: (
      <>
        BI tools, IDEs, Cloud authentication, …
      </>
    ),
  },
  {
    title: 'Administration',
    url: '../../administration/',
    description: (
      <>
        Scale, backups, roles and privileges, …
      </>
    ),
  },
  {
    title: 'Reference',
    url: '../../reference/',
    description: (
      <>
        SQL, functions, error codes, …
      </>
    ),
  },
  {
    title: 'FAQs',
    url: '../../faq/',
    description: (
      <>
        Frequently asked questions.
      </>
    ),
  },
];

function Feature({url, title, description}) {
  return (
    <div className={clsx('col col--6 margin-bottom--lg')}>
     <Link href={url} target="_self" className="card padding--lg cardContainer_fWXF">
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
     </Link>
    </div>
  );
}


export default function Features({language}) {
  if (language == "Chinese") {
    return (
      <section className={styles.features}>
        <div className="container">
          <div className="row">
            {ChineseFeatureList.map((props, idx) => (
              <Feature key={idx} {...props} />
            ))}
          </div>
        </div>
      </section>
    );
  }
  else{
    return (
      <section className={styles.features}>
        <div className="container">
          <div className="row">
            {EnglishFeatureList.map((props, idx) => (
              <Feature key={idx} {...props} />
            ))}
          </div>
        </div>
      </section>
    );
  }
}
