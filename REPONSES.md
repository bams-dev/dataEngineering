# RÉPONSES THÉORIQUES — Évaluation Spark DataFrame API

**Étudiant :** [BAMBA INZA]  
**Date de rendu : 21 Juin 2026**

>  Les questions  **débutant** sont du CODE à compléter dans `EvaluationSpark.scala`.
> Ce fichier contient uniquement les questions  **intermédiaires** et  **expert**.

---

## Exercice 1 — Pipeline d'ingestion & Filtrage

###  Expert — Modes de lecture : PERMISSIVE / DROPMALFORMED / FAILFAST

> *Expliquez la différence entre les trois modes. Quand utiliser chacun en production ?*
Par défaut, Spark utilise le mode PERMISSIVE. Si une ligne est corrompue, le calcul ne s'arrête pas. Spark met simplement des valeurs vides (null) là où il ne peut pas lire, puis copie la ligne entière contenant l'erreur dans une colonne spéciale (columnNameOfCorruptRecord) pour que vous puissiez la vérifier plus tard.
Avec le mode DROPMALFORMED, Spark jette directement à la poubelle les lignes contenant des erreurs ou qui ne correspondent pas à la structure attendue, sans afficher de message d'erreur. À la fin, votre tableau de données ne contient que les lignes qui sont parfaitement correctes.
Le mode FAILFAST est le plus strict. Dès que Spark trouve une seule erreur ou une ligne malformée, il bloque tout, affiche un message d'erreur (RuntimeException) et arrête immédiatement le travail. C'est idéal pour repérer tout de suite les problèmes dans vos fichiers.
---

## Exercice 2 — Jointures & Segmentation

###  Intermédiaire — left_semi vs left_anti

> *Expliquez la différence entre left_semi et left_anti. Donnez un cas métier concret pour chacun.*
Ces deux méthodes permettent de trier votre premier tableau (DF_A) en fonction d'un deuxième tableau (DF_B), sans jamais coller les colonnes du deuxième tableau à la fin.Avec le left_semi, c'est comme une double vérification : Spark garde une ligne de votre premier tableau uniquement si elle existe aussi dans le deuxième.
Avec le left_anti, c'est l'inverse : Spark garde une ligne de votre premier tableau uniquement si elle est complètement introuvable dans le deuxième. C'est idéal pour chercher des anomalies ou des données manquantes.
---

## Exercice 3 — Restructuration des données

###  Intermédiaire — explode() vs explode_outer()

> *Quelle est la différence ? Quand utiliser l'un plutôt que l'autre ?*
explode() : Sépare les éléments d'un tableau (Array) ou d'une Map sur plusieurs lignes. Attention : si la collection est vide ou null, la ligne entière est définitivement supprimée du DataFrame.explode_outer() : Divise également la collection mais conserve la ligne d'origine si le tableau ou la Map est vide ou null, en attribuant la valeur null à la colonne générée.
---

###  Expert — union() vs unionByName() + allowMissingColumns

> *Expliquez la différence entre union() et unionByName(). Que se passe-t-il sans allowMissingColumns=true si les schémas diffèrent ?*
union() est aveugle : Spark colle le second tableau sous le premier en alignant la première colonne avec la première colonne, la deuxième avec la deuxième, etc. Si vos colonnes ne sont pas dans le même ordre (ex. Âge face à Nom), Spark mélange tout ou plante.unionByName() est intelligent : il cherche les colonnes qui portent le même nom pour les aligner correctement, même si elles sont mélangées.Le problème ? Si un tableau possède une colonne que l'autre n'a pas, Spark panique et s'arrête.La solution : allowMissingColumns=True. Cette option autorise la fusion. Spark crée simplement la colonne manquante là où elle n'existait pas et la remplit avec du vide (null).
---

## Exercice 4 — Analyse avancée & Window Functions

###  Intermédiaire — rowsBetween vs rangeBetween

> *Expliquez rowsBetween(unboundedPreceding, 0). Quelle est la différence avec rangeBetween ? Illustrez avec des données numériques.*
rowsBetween vs rangeBetween : Cadre physique vs logiqueL'expression unboundedPreceding associée à la borne 0 (ou Window.currentRow) délimite le cadre de la fenêtre (Frame) de deux manières distinctes :rowsBetween : Découpage physique basé strictement sur l'index de position des lignes. La borne 0 cible uniquement la ligne courante.rangeBetween : Découpage logique basé sur la valeur réelle de la colonne de tri (orderBy). La borne 0 englobe toutes les lignes partageant exactement la même valeur que la ligne courante.
---

###  Expert — groupBy vs Window pour le running total

> *Pourquoi ne peut-on PAS utiliser un simple groupBy pour calculer un running total ? Quel résultat incorrect obtiendrait-on ? Illustrez avec des données chiffrées.*
groupBy vs Window : Destruction vs Préservation de la granularitégroupBy : Écrase le détail des lignes pour fusionner les données selon une clé d'agrégation. On change d'échelle (ex. passer du détail d'une Vente à un total par Client), ce qui rend impossible le suivi chronologique ligne par ligne.Window (Fenêtrage) : Exécute des calculs agrégés ou cumulés tout en conservant intact le niveau de détail initial. Chaque ligne de transaction originale reste pleinement visible dans le DataFrame final.
---

*Fin du fichier REPONSES.md*
