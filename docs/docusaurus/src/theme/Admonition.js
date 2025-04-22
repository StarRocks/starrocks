import React from 'react';
import Admonition from '@theme-original/Admonition';
import MyCustomExperimentalIcon from '@site/static/img/ExperimentalBadge.svg';
import MyCustomBetaIcon from '@site/static/img/BetaBadge.svg';

export default function AdmonitionWrapper(props) {
    if (props.type === 'experimental') {
        return <Admonition title={'Experimental feature'} icon={<span className='text-2xl'><MyCustomExperimentalIcon /></span>}
        >
            {props.children}
        </Admonition>
    } else if (props.type === 'beta') {
    return <Admonition title={'Beta feature'} icon={<span className='text-2xl'><MyCustomBetaIcon /></span>}
    >
      {props.children}
    </Admonition>
  } else {
    return <Admonition {...props} />;
  }
}
