# RÉPONSES THÉORIQUES — Évaluation Spark DataFrame API

**Étudiant :** [Votre Prénom NOM]  
**Date de rendu :**

>  Les questions  **débutant** sont du CODE à compléter dans `EvaluationSpark.scala`.
> Ce fichier contient uniquement les questions  **intermédiaires** et  **expert**.

---

## Exercice 1 — Pipeline d'ingestion & Filtrage

###  Expert — Modes de lecture : PERMISSIVE / DROPMALFORMED / FAILFAST

> *Expliquez la différence entre les trois modes. Quand utiliser chacun en production ?*

---

## Exercice 2 — Jointures & Segmentation

###  Intermédiaire — left_semi vs left_anti

> *Expliquez la différence entre left_semi et left_anti. Donnez un cas métier concret pour chacun.*

---

## Exercice 3 — Restructuration des données

###  Intermédiaire — explode() vs explode_outer()

> *Quelle est la différence ? Quand utiliser l'un plutôt que l'autre ?*

---

###  Expert — union() vs unionByName() + allowMissingColumns

> *Expliquez la différence entre union() et unionByName(). Que se passe-t-il sans allowMissingColumns=true si les schémas diffèrent ?*

---

## Exercice 4 — Analyse avancée & Window Functions

###  Intermédiaire — rowsBetween vs rangeBetween

> *Expliquez rowsBetween(unboundedPreceding, 0). Quelle est la différence avec rangeBetween ? Illustrez avec des données numériques.*

---

###  Expert — groupBy vs Window pour le running total

> *Pourquoi ne peut-on PAS utiliser un simple groupBy pour calculer un running total ? Quel résultat incorrect obtiendrait-on ? Illustrez avec des données chiffrées.*

---

*Fin du fichier REPONSES.md*
