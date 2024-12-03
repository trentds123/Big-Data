# Triangle Counting in Facebook Dataset

This project implements a Spark-based solution to count triangles in a graph represented as an edge list. The dataset used is the `/datasets/facebook` edge list, where each line represents a connection between two nodes.

---

## Table of Contents
1. [Overview](#overview)
2. [Dataset Description](#dataset-description)
3. [How It Works](#how-it-works)
4. [Functions](#functions)
    - [getSC()](#getsc)
    - [getFB()](#getfb)
    - [makeRedundant()](#makeredundant)
    - [noSelfEdges()](#noselfedges)
    - [friendsOfFriends()](#friendsoffriends)
    - [journeyHome()](#journeyhome)
    - [countTriangles()](#counttriangles)
    - [toyGraph()](#toygraph)
5. [Running the Project](#running-the-project)
6. [Output](#output)
7. [Testing](#testing)

---

## Overview
This project processes an edge list to find and count triangles in the graph. A triangle is a set of three nodes connected to each other. Spark transformations and actions are used for distributed processing, making the solution scalable.

---

## Dataset Description
- **Dataset Path**: `/datasets/facebook`
- **Format**: Each line contains a pair of node IDs separated by a space. For example:



- **Graph Representation**:
- Each line represents an edge in the graph.
- Self-loops (e.g., `123 123`) are removed.
- The graph is made undirected by ensuring redundant edges (e.g., both `123 abc` and `abc 123` are included).

---

## How It Works
The solution follows these steps:
1. Load the edge list dataset.
2. Remove self-loops.
3. Make the graph undirected by creating redundant edges.
4. Generate all "friends-of-friends" pairs.
5. Verify which pairs form triangles.
6. Count the triangles and adjust for overcounting.

---

## Functions

### `getSC()`
Creates and returns a `SparkContext` for distributed processing.

### `getFB(sc: SparkContext): RDD[(String, String)]`
- **Purpose**: Load the Facebook dataset and return an RDD of node pairs.
- **Steps**:
- Read the dataset.
- Trim whitespace and split by space.
- Validate and map lines into tuples.

### `makeRedundant(edgeList: RDD[(String, String)]): RDD[(String, String)]`
- **Purpose**: Ensure the graph is undirected by adding reverse edges.
- **Steps**:
- For each edge `(a, b)`, add `(b, a)`.
- Use `distinct()` to remove duplicates.

### `noSelfEdges(edgeList: RDD[(String, String)]): RDD[(String, String)]`
- **Purpose**: Remove self-loops.
- **Steps**:
- Filter edges where both nodes are the same (e.g., `(a, a)`).

### `friendsOfFriends(edgeRDD: RDD[(String, String)]): RDD[(String, (String, String))]`
- **Purpose**: Generate pairs of nodes connected by a common friend.
- **Steps**:
- Group edges by the first node to find all neighbors.
- Generate all unique pairs of neighbors.

### `journeyHome(edgeList: RDD[(String, String)], twoPaths: RDD[(String, (String, String))]): RDD[((String, String), String)]`
- **Purpose**: Verify which friend pairs are directly connected to form triangles.
- **Steps**:
- Reverse the edge list for join operations.
- Join `twoPaths` with the reversed edges.
- Extract pairs of friends forming triangles.

### `countTriangles(edgeList: RDD[(String, String)]): Long`
- **Purpose**: Count triangles in the graph.
- **Steps**:
1. Remove self-loops.
2. Make the graph undirected.
3. Find "friends-of-friends" pairs.
4. Verify triangles.
5. Divide by 3 to adjust for overcounting.

### `toyGraph(sc: SparkContext): RDD[(String, String)]`
- **Purpose**: Generate a small test graph for validation.
- **Output**:
