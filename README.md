# **Spark**
This repository consists of codes that I used to learn [Apache-Spark](https://spark.apache.org/), during my trainee period at Fusemachines Nepal.

In order to use this repository, you must install few Python packages using the command:
```
$ pip install -r requirements.txt
```

Following are the folders contained in the repository:
1. [`practice_codes`](practice_codes/) -
It consists of all the practice codes provided by mentors as well as codes from the book [Spark: The Definitive Guide](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/)

2. [`tasks`](tasks/) -
It consists of few learning tasks given by mentors.

3. [`assignments`](assignments/) -
It consists of assignments i.e. querying datasets to answer some questions.

For all the tasks `data` folder provides the required data and `output` folder has outputs within `assignments` folder.


## **Assignments**
Assignments involved loading data from `json` into Apache-Spark as dataframes and perform transformations on dataframes to answer few specific questions. Assingments on two distinct datasets i.e. [`tweets`](/assignments/data/tweets.json) and [`sales`](assignments/data/sales_records.json).

The result received were written to `csv` files in [`output`](assignments/output/) folder as well as to a PostgreSQL database named `Spark`. Specifically the `.py` files result to inserting into database whereas `.ipynb` files have codes to only write to `csv` files.

### **Setup JDBC**

After installing PostgreSQL, download the postgresql jar file from the following link: [https://jdbc.postgresql.org/download/](https://jdbc.postgresql.org/download/) and paste it in `/usr/lib/jvm/[java_folder]/lib/`

### **Running spark-submit**
Run the following while doing spark-submit in for `.py` files in [`assignments`](assignments/):
```
$ spark-submit --driver-class-path <path_to_jar.jar> <python_file.py>
```