# Crée la variable de profil de coût par stratification (Courbe des coûts).

create_cost_profile <- function(
    df, 
    cost_col, 
    low_cost_threshold = 1/3, 
    moderate_cost_threshold = 2/3
) {
  
  # Assurez-vous que la colonne de coût est un symbole pour la manipulation dplyr
  cost_col_sym <- sym(cost_col)
  
  # 1. Calcul du profil dans un DataFrame temporaire
  profile_map <- df %>%
    # Créer un identifiant de ligne temporaire pour la jointure
    mutate(DUPERSID = DUPERSID) %>%
    
    # Trier par la colonne de coût
    arrange(!!cost_col_sym) %>%
    
    # Calcul des parts cumulées
    mutate(
      cum_cost = cumsum(!!cost_col_sym),
      total_cost = sum(!!cost_col_sym, na.rm = TRUE),
      cum_cost_share = cum_cost / total_cost
    ) %>%
    
    # Assignation du profil
    mutate(
      profil = case_when(
        # Individus à faible coût 
        cum_cost_share <= low_cost_threshold ~ 0,
        # Individus à coût modéré 
        cum_cost_share <= moderate_cost_threshold ~ 1,
        # Individus à coût élevé 
        TRUE ~ 2 
      ),
      profil_label = case_when(
        profil == 0 ~ "Faible Coût",
        profil == 1 ~ "Coût Modéré",
        TRUE ~ "Coût Élevé"
      )
    ) %>%
    # Sélectionner uniquement les identifiants et les profils
    select(DUPERSID, profil, profil_label)
  
  # 2. Joindre le profil au DataFrame original
  df_result <- df %>%
    left_join(profile_map, by = "DUPERSID")
  
  return(df_result)
}


# Création de la variable 'profil' en utilisant TOTEXP23 comme colonne de coût
all_expenses_with_profile <- create_cost_profile(
  df = all_expenses_clean, 
  cost_col = "TOTEXP23" 
  # low_cost_threshold = 0.2 et moderate_cost_threshold = 0.4 sont les valeurs par défaut
)

# Vérification (la distribution devrait être de 20%, 20%, 60%)
cat("\nDistribution des individus par Profil de Coût:\n")
print(table(all_expenses_with_profile$profil_label))
write.csv(all_expenses_with_profile, "all_expenses_saison_profil.csv", row.names = FALSE)


# Cas agrege
agg_expenses_with_profile <- create_cost_profile(
  df = df_modele_depenses, 
  cost_col = "TOTEXP23" 
  # low_cost_threshold = 0.2 et moderate_cost_threshold = 0.4 sont les valeurs par défaut
)

# Vérification (la distribution devrait être de 20%, 20%, 60%)
cat("\nDistribution des individus par Profil de Coût:\n")
print(table(agg_expenses_with_profile$profil_label))
write.csv(agg_expenses_with_profile, "all_expenses_agrege_profil.csv", row.names = FALSE)
