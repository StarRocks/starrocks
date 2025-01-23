
If there is a time zone error during the configuration of **Center service**, configure your timezone on the CelerData Manager server. For example, to configure your server for `America/New_York`:

1. Add this line to the ~/.bashrc for the user that CelerData Manager runs as:
  ```bash
  export TZ = 'America/New_York'
  ```

2. Source the file
  ```bash
  source ~/.bashrc
  ```

3. Before installing the **Web** service, confirm that the time zone is correct:

  ```bash
  export TZ=America/New_York
  [srsandbox4 /]~ date
  Wed Jan 22 11:32:38 EST 2025
  ```

