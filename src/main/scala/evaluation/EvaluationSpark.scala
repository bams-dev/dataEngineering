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
    exercice1_expert(spark, ventesCsvPath).show(10, truncate = false)

    // ════════════════════════════════════════════════════════
    // EXERCICE 2 — Jointures & Segmentation client
    // ════════════════════════════════════════════════════════
    println("\n" + "─" * 65)
    println("[EX 2 - Partie A] Inner Join + Segmentation")
    println("─" * 65)
    exercice2_debutant(spark, ventesDF, clientsDF).show(10, truncate = false)

    println("\n[EX 2 - Partie B] Left Join + Union des mouvements")
    println("─" * 65)
    exercice2_intermediaire(spark, ventesDF, clientsDF, retournsDF).show(10, truncate = false)

    println("\n[EX 2 - Partie C] Semi Join / Anti Join")
    println("─" * 65)
    exercice2_expert(spark, ventesDF, retournsDF).show(10, truncate = false)

    // ════════════════════════════════════════════════════════
    // EXERCICE 3 — Restructuration
    // ════════════════════════════════════════════════════════
    println("\n" + "─" * 65)
    println("[EX 3 - Partie A] Tableau Croisé (Pivot)")
    println("─" * 65)
    exercice3_debutant(spark, ventesDF, produitsDF).show(10, truncate = false)

    println("\n[EX 3 - Partie B] Premier Mot du Motif (Posexplode)")
    println("─" * 65)
    exercice3_intermediaire(spark, ventesDF, retournsDF).show(10, truncate = false)

    println("\n[EX 3 - Partie C] Unpivot Financier (Stack)")
    println("─" * 65)
    exercice3_expert(spark, ventesDF, produitsDF).show(10, truncate = false)

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

    val dfOriginal = spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .csv(csvPath)

    val dfCaster = dfOriginal
      .withColumn("vente_id", col("vente_id").cast(StringType))
      .withColumn("date_vente", col("date_vente").cast(DateType))
      .withColumn("montant_fcfa", col("montant_fcfa").cast(LongType))
      .withColumn("quantite", col("quantite").cast(IntegerType))
      .withColumn("statut", col("statut").cast(StringType))

    val nbNulls = dfCaster.filter(col("montant_fcfa").isNull).count()
    println(s"Nombre de lignes avec montant_fcfa à null : $nbNulls")

    dfCaster
      .withColumn("est_corrige", col("montant_fcfa").isNull)
      .na.fill(Map("montant_fcfa" -> 0L))
      .select("vente_id", "date_vente", "montant_fcfa", "statut", "est_corrige")
  }

  // ══════════════════════════════════════════════════════════
  // EXERCICE 2 — Jointures & Segmentation client (5 pts)
  // ══════════════════════════════════════════════════════════

  /**
   * Partie A — Debutant (1 pt) — inner join + when/otherwise
   */
  def exercice2_debutant(
                          spark: SparkSession,
                          ventesDF: DataFrame,
                          clientsDF: DataFrame
                        ): DataFrame = {
    import org.apache.spark.sql.functions._

    ventesDF.join(clientsDF, "client_id")
      .groupBy("client_id", "client_nom", "ville")
      .agg(sum("montant_fcfa").as("ca_total"))
      .withColumn("segment",
        when(col("ca_total") >= 1000000, "GOLD")
          .when(col("ca_total") >= 400000, "SILVER")
          .otherwise("BRONZE")
      )
      .select("client_id", "client_nom", "ville", "ca_total", "segment")
      .sort(col("ca_total").desc)
  }

  /**
   * Partie B — Intermediaire (1 pt) — left join + union
   */
  def exercice2_intermediaire(
                               spark: SparkSession,
                               ventesDF: DataFrame,
                               clientsDF: DataFrame,
                               retournsDF: DataFrame
                             ): DataFrame = {
    import org.apache.spark.sql.functions._

    val dfVentes = ventesDF.join(clientsDF, Seq("client_id"), "left")
      .filter(col("statut") === "livree")
      .withColumn("type_mouvement", lit("VENTE"))
      .select(
        col("client_id"),
        col("client_nom"),
        col("date_vente").as("date_mouvement"),
        col("montant_fcfa").as("montant"),
        col("type_mouvement")
      )

    val dfRetours = retournsDF
      .withColumn("type_mouvement", lit("RETOUR"))
      .withColumn("montant", -col("montant_retour"))
      .select(
        col("client_id"),
        lit(null).cast("string").as("client_nom"),
        col("date_retour").as("date_mouvement"),
        col("montant"),
        col("type_mouvement")
      )

    dfVentes.unionByName(dfRetours)
      .sort(col("client_id").asc, col("date_mouvement").asc)
  }

  /**
   * Partie C — Expert (1 pt) — Semi join & Anti join
   */
  def exercice2_expert(
                        spark: SparkSession,
                        ventesDF: DataFrame,
                        retournsDF: DataFrame
                      ): DataFrame = {
    import org.apache.spark.sql.functions._

    val acheteursAvecRetour = ventesDF.join(retournsDF, Seq("client_id"), "left_semi")
      .withColumn("profil", lit("ACHETEUR+RETOUR"))
      .select("client_id", "profil")
      .distinct()

    val acheteursFideles = ventesDF.join(retournsDF, Seq("client_id"), "left_anti")
      .withColumn("profil", lit("ACHETEUR_FIDELE"))
      .select("client_id", "profil")
      .distinct()

    acheteursAvecRetour.unionByName(acheteursFideles)
      .sort(col("profil").asc, col("client_id").asc)
  }

  // ══════════════════════════════════════════════════════════
  // EXERCICE 3 — Restructuration (5 pts)
  // Thèmes : pivot, posexplode, unionByName, stack() unpivot
  // ══════════════════════════════════════════════════════════

  /**
   * Partie A — Debutant (1 pt) — pivot correct + na.fill
   * * Objectif : Créer un tableau croisé du montant total des ventes par catégorie et par statut.
   * * Colonnes attendues : categorie | annulee | en_cours | livree
   */
  def exercice3_debutant(spark: SparkSession, ventesDF: DataFrame, produitsDF: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._

    ventesDF.join(produitsDF, "produit_id")
      .groupBy("categorie")
      .pivot("statut")
      .agg(sum("montant_fcfa"))
      .na.fill(0L)
      .sort(col("categorie").asc)
  }

  /**
   * Partie B — Intermediaire (1 pt) — posexplode + filtre pos = 0
   * * Colonnes attendues : vente_id | premier_mot_motif | montant_fcfa
   */
  def exercice3_intermediaire(spark: SparkSession, ventesDF: DataFrame, retournsDF: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._

    // Jointure explicite car le nom de la colonne diffère à droite (vente_id_origine)
    val dfJoint = ventesDF.join(retournsDF, ventesDF("vente_id") === retournsDF("vente_id_origine"))

    dfJoint.select(
        ventesDF("vente_id"),
        posexplode(split(col("motif"), " ")).as(Seq("pos", "mot")),
        col("montant_fcfa")
      )
      .filter(col("pos") === 0)
      .withColumnRenamed("mot", "premier_mot_motif")
      .select("vente_id", "premier_mot_motif", "montant_fcfa")
  }
  /**
   * Partie C — Expert (1 pt) — unionByName + stack() unpivot
    Passeons les colonnes de métriques en lignes (Unpivot).
   * * Colonnes attendues : categorie | indicateur_financier | valeur
   */
  def exercice3_expert(spark: SparkSession, ventesDF: DataFrame, produitsDF: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._

    val dfAgrege = ventesDF.join(produitsDF, "produit_id")
      .groupBy("categorie")
      .agg(
        sum("montant_fcfa").cast(LongType).as("total_recettes"),
        avg("montant_fcfa").cast(LongType).as("moyenne_recettes")
      )

    dfAgrege.select(
        col("categorie"),
        expr("stack(2, 'TOTAL_RECETTES', total_recettes, 'MOYENNE_RECETTES', moyenne_recettes)")
          .as(Seq("indicateur_financier", "valeur"))
      )
      .sort(col("categorie").asc, col("indicateur_financier").desc)
  }
} // <--- L'accolade finale fermant l'objet est bien ici maintenant !