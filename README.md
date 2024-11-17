**_disclaimer_** _I didnt have correct dev environment, node.js install caused lot of issues for me (now I understand they were related to PS script execute permissions and PATH variables), 
so trying to configure local enviroment several times, didnt leave me much option to validate syntax in many scenarios_

**For Oracle to Redshift**
*  I created two files
  *  load_data.py
    * For loading data, I am considering 3 different "loadaction"
      * "F" for Full load (overwrite dest)
      * "A" for append
      * "M" for Merge
        * for merge, I used generic column list of Col1,Col2,Col3...  
    * This "loadaction" bariable can be passed to load_data.py at run time for example
      * ```python load_data.py F``` will run the main() with "Full" load option
  * load_cdc.py
    * I have not messed with ORACLE's CDC or GoldenGate, but here I was exploring ideas of how to pull only incremental data
    * per documentation I found online, both CDC/GoldenGate can create "audit" col or table (depends on how they were configured),
    * for that reason I created two queries (line 60 is commented out in load_cdc.py)
    * I hope I am able to show how I will track deletes from source system
   
**For CDK**
* I have to admit, never deployed a CDK, but it seems easy if you have correct environment setup, for this I needed some help from ChatGPT (_cannot lie about it)_ and in the end was not getting the right folder structure that ChatGPT was suggesting, tried reinstalling and messing with npm/pip several times but was not successful, very frustrating
