-- Databricks notebook source
-- Run once: create the schema and the volume that holds the raw CSV

CREATE SCHEMA IF NOT EXISTS workspace.hotel_lakehouse;

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.hotel_lakehouse.raw;

-- COMMAND ----------

-- After this, upload Hotel_Reviews.csv into the volume:
-- Catalog > workspace > hotel_lakehouse > Volumes > raw > Upload to this volume
SHOW VOLUMES IN workspace.hotel_lakehouse;
