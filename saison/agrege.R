
library(dplyr)
library(tidyr)
library(ggplot2)
library(stringr)

# 1. Sélectionner les colonnes DUPERSID et toutes les dépenses saisonnières
# (colonnes qui contiennent la saison mais pas 'total')
seasonal_cols <- names(all_expenses_clean) %>%
  str_subset("^exp_.*_(winter|spring|summer|fall)$")

expense_seasonal_long <- all_expenses_clean %>%
  select(DUPERSID, all_of(seasonal_cols)) %>%
  
  # 2. Transformer le jeu de données en format long (équivalent de melt/unpivot)
  pivot_longer(
    cols = all_of(seasonal_cols),
    names_to = "Variable_Saison",
    values_to = "Depense"
  ) %>%
  
  # 3. Créer les colonnes 'Poste' et 'Saison'
  mutate(
    # Extraire le nom du poste de dépense (ex: exp_dental)
    Poste = str_extract(Variable_Saison, "^exp_[a-z]+"),
    # Extraire le nom de la saison
    Saison = str_extract(Variable_Saison, "winter|spring|summer|fall")
  ) %>%
  
  # 4. Calculer la dépense MOYENNE par Poste et par Saison
  group_by(Poste, Saison) %>%
  summarise(Depense_Moyenne = mean(Depense, na.rm = TRUE), .groups = 'drop') %>%
  
  # 5. Rendre les noms des postes plus lisibles pour le graphique
  mutate(
    Poste = case_when(
      Poste == "exp_dental" ~ "Dentaire",
      Poste == "exp_hospital" ~ "Hospitalisation",
      Poste == "exp_outpatient" ~ "Visites Externes",
      Poste == "exp_office" ~ "Cabinets Médicaux",
      Poste == "exp_er" ~ "Urgences (ER)",
      Poste == "exp_home" ~ "Soins à Domicile",
      Poste == "exp_others" ~ "Autres Achats",
      Poste == "exp_medicines" ~ "Médicaments",
      TRUE ~ Poste
    )
  )




# Définir l'ordre des saisons pour la cohérence
saison_order <- c("winter", "spring", "summer", "fall")
expense_seasonal_long$Saison <- factor(expense_seasonal_long$Saison, levels = saison_order)

# Créer le graphique
ggplot(expense_seasonal_long, aes(x = Saison, y = Depense_Moyenne, fill = Poste)) +
  # Graphique à barres empilées pour voir la composition totale
  geom_bar(stat = "identity", position = "stack") +
  
  # Optionnellement : utiliser facet_wrap pour séparer les postes et comparer les saisons
  # facet_wrap(~ Poste, scales = "free_y") +
  
  labs(
    title = "Dépenses de Santé Moyennes par Saison et par Poste",
    subtitle = "Visualisation de l'impact saisonnier",
    x = "Saison",
    y = "Dépense Moyenne (USD)",
    fill = "Poste de Dépense"
  ) +
  scale_y_continuous(labels = scales::dollar) + # Formatage monétaire (nécessite scales::)
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))




# Créer le graphique
ggplot(expense_seasonal_long, aes(x = Saison, y = Depense_Moyenne, fill = Saison)) +
  geom_bar(stat = "identity") +
  # Séparer un graphique par poste de dépense
  facet_wrap(~ Poste, scales = "free_y", ncol = 4) + 
  
  labs(
    title = "Dépenses de Santé Moyennes par Poste : Variation Saisonnière",
    x = "Saison",
    y = "Dépense Moyenne (USD)",
    fill = "Saison"
  ) +
  scale_y_continuous(labels = scales::dollar) +
  theme_minimal() +
  theme(axis.text.x = element_blank(), # Enlever les labels X des petites facettes
        axis.ticks.x = element_blank())





df_modele_depenses <- all_expenses_clean %>%
  
  # 1. Sélection des colonnes essentielles
  select(
    DUPERSID, 
    TOTEXP22, # Variable cible pour le total des dépenses
    TTLP22X, # Revenu total du foyer (si pertinent pour le modèle)
    FAMINC22, # Revenu familial (si pertinent pour le modèle)
    AGE22X, # Âge
    EDUCYR, # Niveau d'éducation
    # Sélectionnez toutes les colonnes de dépenses totales agrégées
    matches("^exp_.*_total$")
  ) %>%
  
  # 2. Renommage des colonnes de dépenses totales pour plus de clarté
  rename_with(
    ~ str_replace(.x, "exp_", "Total_"), 
    matches("^exp_")
  ) %>%
  rename_with(
    ~ str_remove(.x, "_total$"),
    matches("_total$")
  )

# Afficher les dimensions et un aperçu des colonnes
cat("Dimensions du DataFrame pour la régression :", dim(df_modele_depenses), "\n")
print(head(df_modele_depenses))
print(names(df_modele_depenses))


write.csv(df_modele_depenses, "all_expenses_agrege.csv", row.names = FALSE)
