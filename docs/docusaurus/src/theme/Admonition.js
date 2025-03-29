import React from 'react';
import Admonition from '@theme-original/Admonition';
import MyCustomExperimentalIcon from '@site/static/img/ExperimentalBadge.svg';

export default function AdmonitionWrapper(props) {
  if (props.type === 'experimental') {
    return <Admonition title={<a className="no-underline" href="/docs/experimental_preview">Experimental feature</a>} icon={<span className='text-2xl'><MyCustomExperimentalIcon /></span>}
    >
      {props.children}
    </Admonition>
  } else {
    return <Admonition {...props} />;
  }
}
