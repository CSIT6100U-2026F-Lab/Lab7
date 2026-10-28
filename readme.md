# Lab 7 —  Top-K (Map-Reduce)

## 1. Overview
In lab7, we keep the code file CRUD APIs from Lab 5, and a **Top-K** button has been added. It computes the most frequent words across `data/`.

We have provided a starter code that tries to implement Top-K with map-reduce principle, but it's somehow incorrect. Your task is to correct it.


## 2. Setup

Spark needs a JDK. This lab's PySpark (3.5 or 4.x) requires **JDK 17+**. Make sure `JAVA_HOME` is set:

```bash
echo $JAVA_HOME
java -version
```

`java -version` should print 17 or newer. If `JAVA_HOME` is empty on macOS:

```bash
export JAVA_HOME=$(/usr/libexec/java_home)
```

Install Python packages:

```bash
pip install -r requirements.txt
```

## 3. Start

Start the backend and frontend:
+ the code explorer is on: http://127.0.0.1:5500/

```bash
python3 backend/rest_service.py
python3 frontend/serve.py
```

Note that the first Top-K click starts a JVM, so it could take several seconds.

If a previous run left a port busy:

```bash
lsof -tiTCP:8000,5500 -sTCP:LISTEN | xargs kill
```

## 4. TODO

Find what's wrong with the current map-reduce implementation
+ We have placed TODOs in `map_partition()` and `reduce_counts()`. 


## 5. Evaluation

```bash
python3 test_topk.py
```

We have provided a test script `test_topk.py`:
+ It runs the map-reduce top-k on `data/` for different Ks.
+ The result of each case will be compared with a single-process top-k version.
+ NOTE: Rank by count from high to low. When two words have the same count, break the tie by alphabetical order of the word.

## 6. File structure

```
Lab7/
├── frontend/
│   ├── index.html
│   ├── app.js
│   ├── style.css
│   └── serve.py            # static server on :5500
├── backend/
│   ├── data_logic.py       # disk layer (provided)
│   ├── rest_service.py     # REST on :8000 — CRUD + GET /topk
│   └── map_reduce.py       # Spark job — fill in map_partition
├── data/
├── test_topk.py
└── requirements.txt
```
