### Installation
1. First install all dependencies in requirement.txt using below command
```bash
    pip install -r requirement.txt
```
2. Register API KEYS for 
   * Infura (https://app.infura.io/register)
     * Register new account with your email
     * Enable Ethereum and Binance Smart Chain service (main chain)
   * Etherscan (https://etherscan.io/register)
     * Register new account with your email
     * Create new API key
   * Bscscan (https://bscscan.com/register)
     * Register new account with your email
     * Create new API key
3. Update your keys in [config.ini](resources/config.ini)

### Project Structure
* Data collection: [data_collection](main/data_collection)
* Blockchain APIs: [api](main/api)
* Our algorithms: [algorithms](main/algorithms)
* Data Transfer Object (DTO):  [entity](main/entity)
  * Blockchain DTO: [blockchain](main/entity/blockchain)
  * Our DTO: [entity](main/entity)
* Others: [utils](main/utils)

### Run multiple instances
 * If you are using Pycharm, see this [instruction](https://www.jetbrains.com/help/pycharm/run-debug-multiple.html)
 * If not, please refer [Python MultiThreading](https://www.geeksforgeeks.org/multithreading-python-set-1/)