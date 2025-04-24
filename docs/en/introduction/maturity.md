# Beta and experimental features

StarRocks features have five potential maturity levels:

- Experimental
- Beta
- Generally Available (GA)
- Deprecated
- Removed

Most of the StarRocks features are GA, and if there is no label in the documentation to indicate that a feature is Experimental, Beta, or Deprecated—then the feature is GA.

## Experimental features

- **Stability**: Possibly buggy, with minor known issues.
- **Maturity**: Low
- **Interface**: The interface may be changed in the future. This includes command syntax, configuration parameters, defaults, feature removal, etc.
- **Availability**: Experimental features are off by default, and need to be allowed by setting a parameter with SQL or in a configuration file.
- **Production readiness**: Experimental features should not be used in production.
- **Support**: Please open a [GitHub issue](https://github.com/StarRocks/starrocks/issues) or ask questions in [Slack](https://starrocks.io/redirecting-to-slack) and the StarRocks Engineering team will try to help you.

## Beta features

- **Stability**: Well tested.
May be not good for corner cases.
- **Maturity**: Core functionality is complete, may not be performance-optimized.
- **Interface**: The interface may be changed in the future. May be not backward compatible.
- **Availability**: Beta features are off by default, and need to be allowed by setting a parameter with SQL or in a configuration file.
- **Production readiness**: Beta features are not recommended for production use.
- **Support**: Please open a [GitHub issue](https://github.com/StarRocks/starrocks/issues) or ask questions in [Slack](https://starrocks.io/redirecting-to-slack) and the StarRocks Engineering team will try to help you.

## GA features

- **Stability**: Comprehensively tested.
- **Maturity**: High.
- **Interface**: Stable API.
- **Availability**: GA features are on by default.
- **Production readiness**: Production ready.
- **Support**: The support team provides support to customers. Open-source community members should open a [GitHub issue](https://github.com/StarRocks/starrocks/issues) or ask questions in [Slack](https://starrocks.io/redirecting-to-slack) and the StarRocks Engineering team will try to help you.

## Deprecated features

Some features are deprecated—marked for removal, because they are replaced with other features or the features were not being used. Generally when a feature is deprecated we will suggest an alternative in the documentation.
