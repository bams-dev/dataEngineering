package evaluation

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

/**
 * ════════════════════════════════════════════════════════════
 * ÉVALUATION APACHE SPARK — Master 2 Big Data
 * DataFrame API — Thèmes mixtes
 * ════════════════════════════════════════════════════════════
 *
 * ÉTUDIANT  : [BAMBA INZA]
 * RENDU     : Lundi matin avant 12h00 via git push
 *
 * FICHIERS À COMPLÉTER :
 * - EvaluationSpark.scala  (ce fichier)
 * - REPONSES.md            (questions théoriques)
 *
 * LANCEMENT : sbt run   ou   clic droit → Run dans IntelliJ
 * ════════════════════════════════════════════════════════════
 */
object EvaluationSpark {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Evaluation-Spark-M2")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=" * 65)
    println("  ÉVALUATION SPARK M2 — DataFrame API Thèmes mixtes")
    println("=" * 65)

    // Chemin du fichier CSV pour l'exercice 1 Partie C
    val ventesCsvPath = "src/main/resources/ventes.csv"

    // ── Chargement ──────────────────────────────────────────
    val ventesDF = chargerVentes(spark)
    val clientsDF = chargerClients(spark)
    val produitsDF = chargerProduits(spark)
    val retournsDF = chargerRetours(spark)

    println(s"\n>>> ventes.csv   : ${ventesDF.count()} lignes")
    println(s">>> clients.csv  : ${clientsDF.count()} lignes")
    println(s">>> produits.csv : ${produitsDF.count()} lignes")
    println(s">>> retours.csv  : ${retournsDF.count()} lignes")

    // ════════════════════════════════════════════════════════
    // EXERCICE 1 — Pipeline d'ingestion & Filtrage
    // ════════════════════════════════════════════════════════
    println("\n" + "─" * 65)
    println("[EX 1 - Partie A] Sélection et filtre basique")
    println("─" * 65)
    exercice1_debutant(spark, ventesDF).show(10, truncate = false)

    println("\n[EX 1 - Partie B] Filtres avancés (isin, between, like)")
    println("─" * 65)
    exercice1_intermediaire(spark, ventesDF, produitsDF).show(10, truncate = false)

    println("\n[EX 1 - Partie C] Schema explicite + gestion des nulls")
    println("─" * 65)
    // Passage du chemin d'accès au fichier pour permettre le rechargement explicite
    exercice1_expert(spark, ventesCsvPath).show(10, truncate = false)
    spark.stop()
  }

  // ══════════════════════════════════════════════════════════
  // CHARGEMENT — ne pas modifier
  // ══════════════════════════════════════════════════════════

  def chargerVentes(spark: SparkSession): DataFrame =
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv("src/main/resources/ventes.csv")

  def chargerClients(spark: SparkSession): DataFrame =
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv("src/main/resources/clients.csv")

  def chargerProduits(spark: SparkSession): DataFrame =
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv("src/main/resources/produits.csv")

  def chargerRetours(spark: SparkSession): DataFrame =
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv("src/main/resources/retours.csv")

  // ══════════════════════════════════════════════════════════
  // EXERCICE 1 — Pipeline d'ingestion & Filtrage
  // ══════════════════════════════════════════════════════════

  /**
   * Partie A — Debutant (1 pt)
   */
  def exercice1_debutant(spark: SparkSession, ventesDF: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._

    ventesDF
      .filter(col("statut") === "livree")
      .withColumn("montant_ttc", round(col("montant_fcfa") * 1.18, 0))
      .select("vente_id", "date_vente", "produit_id", "montant_fcfa", "montant_ttc")
      .sort(col("montant_ttc").desc)
  }

  /**
   * Partie B — Intermediaire (1 pt)
   */
  def exercice1_intermediaire(
                               spark: SparkSession,
                               ventesDF: DataFrame,
                               produitsDF: DataFrame
                             ): DataFrame = {
    import org.apache.spark.sql.functions._

    ventesDF.join(produitsDF, "produit_id")
      .filter(
        col("categorie").isin("Informatique", "Téléphonie", "Stockage") &&
          col("montant_fcfa").between(50000, 500000) &&
          col("produit_nom").rlike("^[SL]")
      )
      .withColumn("cat_court", substring(col("categorie"), 1, 3))
      .select("vente_id", "produit_nom", "cat_court", "categorie", "montant_fcfa")
      .sort(col("categorie").asc, col("montant_fcfa").desc)
  }

  /**
   * Partie C — Expert (1 pt) — Schéma explicite + nulls + casting
   */
  def exercice1_expert(spark: SparkSession, csvPath: String): DataFrame = {
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._

    // 1. On charge d'abord en mode PERMISSIVE avec inférence automatique
    // pour ne pas casser l'ordre naturel des colonnes du fichier
    val dfOriginal = spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .csv(csvPath)

    // 2. On applique un cast explicite sur les types
    val dfCaster = dfOriginal
      .withColumn("vente_id", col("vente_id").cast(StringType))
      .withColumn("date_vente", col("date_vente").cast(DateType))
      .withColumn("montant_fcfa", col("montant_fcfa").cast(LongType))
      .withColumn("quantite", col("quantite").cast(IntegerType))
      .withColumn("statut", col("statut").cast(StringType))

    // 3. Compter le nombre exact de vraies lignes dont le montant est null
    val nbNulls = dfCaster.filter(col("montant_fcfa").isNull).count()
    println(s"Nombre de lignes avec montant_fcfa à null : $nbNulls")

    // 4 & 5. Remplacer les nulls par 0 et ajouter l'indicateur de correction
    dfCaster
      .withColumn("est_corrige", col("montant_fcfa").isNull)
      .na.fill(Map("montant_fcfa" -> 0L))
      .select("vente_id", "date_vente", "montant_fcfa", "statut", "est_corrige")

}

}