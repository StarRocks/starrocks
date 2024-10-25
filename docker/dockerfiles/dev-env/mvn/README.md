Specific settings to maven utility so that it can find a different location for its local repository.

* an additional option `-s /root/.mvn/settings.xml` is added in `maven.config` when mvn command is invoked.
* `/root/.mvn/settings.xml` configures mvn to use `/root/.mvn/repositories` as its local repository location.
