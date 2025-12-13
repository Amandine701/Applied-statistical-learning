library(haven)
library(dplyr)
library(tidyr)
library(readr)


load_meps_zip <- function(urls, zipnames, objnames, outdir = "meps_data") {
  
  #création du dossier outdir
  dir.create(outdir, showWarnings = FALSE)
  
  for (i in seq_along(urls)) {
    url      <- urls[i]
    zipfile  <- file.path(outdir, zipnames[i])
    objname  <- objnames[i]
    
    #  Téléchargement du fichier zip
    download.file(url, destfile = zipfile, mode = "wb")
    
    #  Décompression et extraction du fichier .dta
    unzip_dir <- file.path(outdir, tools::file_path_sans_ext(zipnames[i]))
    dir.create(unzip_dir, showWarnings = FALSE)
    unzip(zipfile, exdir = unzip_dir)
    dta_file <- list.files(unzip_dir, pattern = "\\.dta$", full.names = TRUE)
    
    # Nous téléchargeons le fichier .dta et le renommons
    assign(objname, read_dta(dta_file), envir = .GlobalEnv)
  }
}

# Application de la fonction -------------------------------------------

urls <- c(
  "https://meps.ahrq.gov/mepsweb/data_files/pufs/h243/h243dta.zip",
  "https://meps.ahrq.gov/mepsweb/data_files/pufs/h252/h252dta.zip",
  "https://meps.ahrq.gov/mepsweb/data_files/pufs/h251/h251dta.zip",
  "https://meps.ahrq.gov/mepsweb/data_files/pufs/h239a/h239adta.zip",
  "https://meps.ahrq.gov/mepsweb/data_files/pufs/h239b/h239bdta.zip",
  "https://meps.ahrq.gov/mepsweb/data_files/pufs/h239c/h239cdta.zip",
  "https://meps.ahrq.gov/mepsweb/data_files/pufs/h239d/h239ddta.zip",
  "https://meps.ahrq.gov/mepsweb/data_files/pufs/h239e/h239edta.zip",
  "https://meps.ahrq.gov/mepsweb/data_files/pufs/h239f/h239fdta.zip",
  "https://meps.ahrq.gov/mepsweb/data_files/pufs/h239g/h239gdta.zip",
  "https://meps.ahrq.gov/mepsweb/data_files/pufs/h239h/h239hdta.zip"
)

zipnames <- c("h243.zip", "h252.zip", "h251.zip", "h239a.dta", "h239b.dta",
              "h239c.dta", "h239d.dta", "h239e.dta", "h239f.dta", "h239g.dta",
              "h239h.dta")

objnames <- c("full_year_2022", "longitudinal", "full_year_2023",
              "prescribed_medicines_2022", "dental_2022","others_2022",
              "hospitals_2022", "emergency_room_2022", "outpatients_visits_2022",
              "Office_Based_Medical_Provider_2022", "home_health_2022")

load_meps_zip(urls, zipnames, objnames)




# Nous agrégeons les dépenses pour chaque individu et pour chaque mois
# dans les fichiers détaillant les dépenses par poste de façon à obtenir
# - les dépenses mensuelles de chaque individu par poste
#(variables de type exp_poste_1, exp_poste2... pour le poste "poste")
# - une variable détaillant la dépense totale en 2022 pour ce poste pour chaque
# individu (variable exp_poste_total)

# Les cinq premiers fichiers sont traités de façon analogue
%%R
library(dplyr)
library(tidyr)

# Fonction générique pour agréger les dépenses par saison
aggregate_by_season <- function(data, date_col, exp_col, prefix) {
  # Nom des df et colonnes
  date_col_sym <- sym(date_col)
  exp_col_sym <- sym(exp_col)
  total_col_name <- paste0(prefix, "total")
  
  result <- data %>%
    #nous créons une variable de saison
    mutate(
      SAISON = case_when(
        !!date_col_sym %in% c(1, 2, 3) ~ "winter",
        !!date_col_sym %in% c(4, 5, 6) ~ "spring",
        !!date_col_sym %in% c(7, 8, 9) ~ "summer",
        !!date_col_sym %in% c(10, 11, 12) ~ "fall",
        TRUE ~ NA_character_
      )
    ) %>%
    #nous sommons les dépenses par saison et par individu
    group_by(DUPERSID, SAISON) %>%
    summarise(
      exp_saison = sum(!!exp_col_sym, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    # Nous créons une colonne de dépense par saison
    pivot_wider(
      names_from = SAISON,
      values_from = exp_saison,
      names_prefix = prefix,
      values_fill = 0 # on remplace les NA (dépense nulle dans une saison) par 0
    ) %>%
    # Nous ajoutons la somme annuelle
    rowwise() %>%
    mutate(
      !!total_col_name := sum(c_across(starts_with(prefix)), na.rm = TRUE)
    ) %>%
    ungroup()
  
  return(result)
}


# Dépenses dentaires
agg_dental <- aggregate_by_season(
  data = dental_2022,
  date_col = "DVDATEMM",
  exp_col = "DVXP22X",
  prefix = "exp_dental_"
)

# Dépenses des visites externes
agg_outpatient <- aggregate_by_season(
  data = outpatients_visits_2022,
  date_col = "OPDATEMM",
  exp_col = "OPXP22X",
  prefix = "exp_outpatient_"
)

# Dépenses des cabinets médicaux
agg_office <- aggregate_by_season(
  data = Office_Based_Medical_Provider_2022,
  date_col = "OBDATEMM",
  exp_col = "OBXP22X",
  prefix = "exp_office_"
)

# Dépenses des urgences
agg_er <- aggregate_by_season(
  data = emergency_room_2022,
  date_col = "ERDATEMM",
  exp_col = "ERXP22X",
  prefix = "exp_er_"
)

# Dépenses de soins à domicile
agg_home <- aggregate_by_season(
  data = home_health_2022,
  date_col = "HHDATEMM",
  exp_col = "HHXP22X",
  prefix = "exp_home_"
)

# Other ----
seasons_list <- c("winter", "spring", "summer", "fall")



aggregate_uniform_seasonal <- function(data, exp_col, prefix) {
  # Nom des df et colonnes
  exp_col_sym <- sym(exp_col)
  total_col_name <- paste0(prefix, "total")
  
  seasons_list <- c("winter", "spring", "summer", "fall")
  
  seasonal_data <- data %>%
    # on calcule la dépense pour chaque saison
    mutate(exp_seasonal = !!exp_col_sym / 4) %>%
    # Création de 4 lignes par individu (une pour chaque saison)
    tidyr::crossing(SAISON = seasons_list) %>%
    group_by(DUPERSID, SAISON) %>%
    summarise(
      # Somme de la dépense saisonnière calculée
      exp_final = sum(exp_seasonal, na.rm = TRUE), 
      .groups = "drop"
    )
  
  
  
  # Calcul de la dépense par saison et la dépense totale pour chaque individu
  agg_data <- seasonal_data %>%
    pivot_wider(
      names_from = SAISON,
      values_from = exp_final,
      names_prefix = prefix,
      values_fill = 0
    ) %>%
    rowwise() %>%
    mutate(
      !!total_col_name := sum(c_across(starts_with(prefix)), na.rm = TRUE)
    ) %>%
    ungroup()
  
  return(agg_data)
}

# Traitement du fichier 'medicines'
agg_medicines <- aggregate_uniform_seasonal(
  data = prescribed_medicines_2022,
  exp_col = "RXXP22X",
  prefix = "exp_medicines_"
)

# Traitement du fichier 'others'
agg_others <- aggregate_uniform_seasonal(
  data = others_2022,
  exp_col = "OMXP22X",
  prefix = "exp_others_"
)


#Hospital ----
# Fonction utilitaire pour mapper le mois à la saison
get_saison_from_month <- function(month) {
  case_when(
    month %in% c(1, 2, 3) ~ "winter",
    month %in% c(4, 5, 6) ~ "spring",
    month %in% c(7, 8, 9) ~ "summer",
    month %in% c(10, 11, 12) ~ "fall",
    TRUE ~ NA_character_
  )
}



hospitals_monthly <- hospitals_2022 %>%
  #On supprime les lignes pour lesquelles la date de fin d'hospitalisation n'est pas connue
  filter(IPENDMM != -8) %>%
  # Calcul du nombre de mois d'hospitalisation selon l'année de début
  mutate(
    n_months = case_when(
      IPBEGYR == 2022 ~ IPENDMM - IPBEGMM + 1,
      IPBEGYR == 2021 ~ (12 - IPBEGMM + 1) + IPENDMM,
      TRUE ~ NA_real_
    ),
    #Calcul de la dépense mensuelle
    exp_hospitals_month = IPXP22X / n_months
  ) %>%
  rowwise() %>%
  #Liste des mois concernés par l'hospitalisation
  mutate(
    month = case_when(
      IPBEGYR == 2022 ~ list(seq(IPBEGMM, IPENDMM)),
      IPBEGYR == 2021 ~ list(seq(1, IPENDMM)), 
      TRUE ~ list(NA_integer_)
    )
  ) %>%
  unnest(month) %>%
  ungroup() %>%
  # Création de la variable SAISON à partir du mois
  mutate(SAISON = get_saison_from_month(month))

# Agrégation des dépenses mensuelles en dépenses saisonnières (pour 2022)
hospitals_seasonal <- hospitals_monthly %>%
  group_by(DUPERSID, SAISON) %>%
  # On somme toutes les contributions mensuelles de chaque saison
  summarise(exp_hospitals = sum(exp_hospitals_month, na.rm = TRUE), .groups = "drop") %>%
  # On complète le dataframe pour s'assurer que toutes les 4 saisons sont présentes (avec 0 si aucune dépense)
  complete(DUPERSID, SAISON = c("winter", "spring", "summer", "fall"), fill = list(exp_hospitals = 0))

# Création de la dépense agrégée finale par saison 
agg_hospital <- hospitals_seasonal %>%
  pivot_wider(
    names_from = SAISON,
    values_from = exp_hospitals,
    names_prefix = "exp_hospitals_",
    values_fill = 0 
  ) %>%
  rowwise() %>%
  # Total de la dépense imputée à 2022
  mutate(
    exp_hospitals_2022_total = sum(
      c_across(starts_with("exp_hospitals_")),
      na.rm = TRUE
    )
  ) %>%
  ungroup()


#  Join ----
# Les variables précédemment créées détaillant les dépenses agrégées par poste
# pour chaque individu sont jointes aux fichiers full_year_2022 et longitudinal
# pour inclure certaines caractéristiques socio-démographiques et économiques à l'étude.

# DUPERSID présents dans hospitals_2022 mais supprimés dans agg_hospitals
# (du fait de l'absence de l'information sur la durée d'hospitalisation)
ids_removed <- hospitals_2022 %>%
  filter(IPENDMM == -8 ) %>%
  pull(DUPERSID)


# CRéation du dataframe avec l'ensemble des dépenses mensuelles par postes
# et les caractéristiques socio-démographiques et économiques
all_expenses <- full_year_2022 %>%
  filter(!DUPERSID %in% ids_removed)  %>%
  select(DUPERSID,  EDUCYR,  TTLP22X, FAMINC22, AGE22X, TOTEXP22  ) %>%
  left_join(full_year_2023 %>% select(DUPERSID,  TOTEXP23), by= "DUPERSID") %>%
  left_join(longitudinal %>% select(DUPERSID), by= "DUPERSID")%>%
  left_join(agg_dental, by="DUPERSID") %>%
  left_join(agg_hospital, by="DUPERSID") %>%
  left_join(agg_outpatient, by="DUPERSID") %>%
  left_join(agg_office, by="DUPERSID") %>%
  left_join(agg_er, by="DUPERSID") %>%
  left_join(agg_home, by="DUPERSID") %>%
  left_join(agg_others, by="DUPERSID") %>%
  left_join(agg_medicines, by="DUPERSID")

# Etude des valeurs négatives
table(all_expenses$EDUCYR)
na_educ <- all_expenses %>% filter(EDUCYR <0)
table(na_educ$AGE22X)
# Il semblerait que cela soit des jeunes
young <- all_expenses %>% filter(0<= AGE22X & AGE22X < 9)
table(young$AGE22X)
young_educ <- young %>% filter(EDUCYR>=0)
table(young_educ$AGE22X)


all_expenses_clean <- all_expenses %>% mutate(
  EDUCYR = ifelse( EDUCYR <0, 0, EDUCYR)
)

library(ggplot2)
plot_data <- all_expenses %>%
  mutate(
    EDUCYR_status = case_when(
      EDUCYR < 0 ~ "EDUCYR Négatif/Inconnu",
      EDUCYR == 0 ~ "EDUCYR = 0 (Aucune éducation)",
      EDUCYR > 0 ~ "EDUCYR Positif",
      TRUE ~ "NA"
    ),
    Age_Group = ifelse(AGE22X < 9, "Très Jeune (0-8 ans)", "Adultes et Enfants (9+ ans)")
  ) %>%
  # On filtre pour les groupes pertinents
  filter(EDUCYR_status != "NA")

density_data <- all_expenses %>%
  # Limiter l'analyse aux individus de moins de 18 ans
  filter(AGE22X < 18) %>% 
  mutate(
    EDUCYR_status = ifelse(EDUCYR < 0, "EDUCYR Négatif/Inconnu", "EDUCYR Non-Négatif (>= 0)")
  )

%%R
# Graphique de densité pour comparer les distributions d'âge
ggplot(density_data, aes(x = AGE22X, fill = EDUCYR_status)) +
  geom_density(alpha = 0.5, adjust = 1) + # adjust=1 pour lisser l'estimation de densité
  scale_fill_manual(values = c("EDUCYR Négatif/Inconnu" = "#E41A1C", 
                               "EDUCYR Non-Négatif (>= 0)" = "#377EB8")) +
  labs(
    title = "Comparaison de la Distribution d'Âge (Moins de 18 ans)",
    subtitle = "EDUCYR Négatif (Cas à imputer à 0) vs. EDUCYR Non-Négatif",
    x = "Âge en 2022 (AGE22X)",
    y = "Densité",
    fill = "Statut de EDUCYR"
  ) +
  theme_minimal()



cat("\n Nombre de lignes total \n")
print(nrow(all_expenses_clean))
cat("\n Nombre de lignes avec revenu négatif \n")
print(nrow(all_expenses_clean %>% filter(TTLP22X <0 | FAMINC22 <0)))
cat("\n Nombre de lignes sans donnée d'âge \n")
print(nrow(all_expenses_clean %>% filter(AGE22X <0)))


all_expenses_clean <- all_expenses_clean %>% 
  filter(TTLP22X >=0 & FAMINC22 >=0 & AGE22X >=0)


# On met des 0 aux dépenses NA
library(stringr)
all_expense_cols <- names(all_expenses_clean) %>% 
  str_subset("^exp_")
all_expenses_clean <- all_expenses_clean %>%
  mutate(across(all_of(all_expense_cols), ~ replace_na(.x, 0))) %>% 
  mutate(TOTEXP23 = replace_na(TOTEXP23, 0))


write.csv(all_expenses_clean, "all_expenses_saison.csv", row.names = FALSE)
