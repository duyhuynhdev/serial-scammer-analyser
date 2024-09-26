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

### S3Syncer
1. Install aws-cli (https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) 
    
2. Run the aws configure command in your terminal. This will prompt you to enter your AWS credentials. If you 
don't have these credentials, reach out to your project manager to obtain them.  
    
3. Instantiate the S3FileManager class by specifying the directory to sync between the S3 bucket and local storage.
       
   For example: s3_file_manager = S3FileManager(data_dir="resources/data/uniswap")

   Note that `data_dir` is the path to the local directory that will be synchronized with the corresponding 
   directory in your S3 bucket.

4. To synchronize files between the S3 bucket and the local directory, simply call the `sync()` method wherever
   necessary in your program:

   s3_file_manager.sync()

   Note that this will ensure that any new or modified files in either the local directory or the S3 bucket are 
   synchronized.