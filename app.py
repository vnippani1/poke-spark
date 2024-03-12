from flask import Flask, request, render_template
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import pandas as pd

app = Flask(__name__)

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

class PokeSpark:
    def __init__(self) -> None:
        self.pkData = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///SparkCourse/pokemon.csv")
        self.pkData = self.pkData.withColumnRenamed("Sp. Atk", "SpAtk").withColumnRenamed("Sp. Def", "SpDef").withColumnRenamed("Type 1","Type1").withColumnRenamed("Type 2","Type2")
        self.pkData = self.pkData.withColumn("BST", func.col("HP") + func.col("Attack") + func.col("Defense") + func.col("SpAtk") + func.col("SpDef") + func.col("Speed"))
    
    def findPokemon(self, firstLetter, type1, type2, gen):
        temp = self.pkData

        if firstLetter != '':
            temp = temp.filter(func.upper(func.substring(temp["Name"], 0, 1)) == firstLetter.upper())

        if type1 != '':
            temp = temp.filter((temp["Type1"] == type1) | (temp["Type2"] == type1))

        if type2 != '':
            temp = temp.filter((temp["Type1"] == type2) | (temp["Type2"] == type2))

        if gen != '':
            temp = temp.filter(temp["Generation"] == gen)

        return temp

poke_spark = PokeSpark()

@app.route('/')
def index():
    return render_template('index.html', pokemon_data=None)

@app.route('/pokemon', methods=['GET'])
def get_pokemon():
    first_letter = request.args.get('first_letter', '')
    type1 = request.args.get('type1', '')
    type2 = request.args.get('type2', '')
    gen = request.args.get('gen', '')

    result = poke_spark.findPokemon(first_letter, type1, type2, gen)
    pandas_df = result.toPandas()

    return render_template('index.html', pokemon_data=pandas_df.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True,port=8000)
