# BDT_code
Giovanni Astante, Letizia Fabbri,  Davide Vandelli
Code for the 2023 Big Data Technologies course project work.

Warnings:
- in order for the code to work, download the 'loc_gb.csv' file. Then you must plug the path where you put the file you've just downloaded into the 'filepath' variable in the 'redis_db.py' script. In the 'filepath' variable you will first find the local path of the machine belonging to who've written this code, substitute it with your path.
- the 'meteo.py' and the 'meteo_spark.py' scripts have the role of defining the classes that will be imported by the 'redis_db.py' script, which is the main file, the only one you have to run.
- Regarding the main file, for storig the JSON into Redis the following Redis module is required: < https://redis.io/docs/data-types/json/ >. However, we did not manage to make the module work on our machine so we incurred into the error: ".ResponseError: unknown command 'JSON.SET'". We could not test the final part of the code.
