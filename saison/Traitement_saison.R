library(haven)
library(dplyr)
library(tidyr)
library(readr)
library(stringr)
setwd("~/work/Applied-statistical-learning")

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

#load_meps_zip(urls, zipnames, objnames)


setwd("~/work/Applied-statistical-learning/")
local <- c(
  "meps_data/h243/h243dta.zip",
  "meps_data/h252/h252dta.zip",
  "meps_data/h251/h251dta.zip",
  "meps_data/h239a/h239adta.zip",
  "meps_data/h239b/h239bdta.zip",
  "meps_data/h239c/h239cdta.zip",
  "meps_data/h239d/h239ddta.zip",
  "meps_data/h239e/h239edta.zip",
  "meps_data/h239f/h239fdta.zip",
  "meps_data/h239g/h239gdta.zip",
  "meps_data/h239h/h239hdta.zip"
)
for (i in seq_along(local)) {
  url      <- urls[i]
  zipfile  <- file.path("meps_data", zipnames[i])
  objname  <- objnames[i]
  
  #  Décompression et extraction du fichier .dta
  unzip_dir <- file.path("meps_data", tools::file_path_sans_ext(zipnames[i]))
  dir.create(unzip_dir, showWarnings = FALSE)
  unzip(zipfile, exdir = unzip_dir)
  dta_file <- list.files(unzip_dir, pattern = "\\.dta$", full.names = TRUE)
  
  # Nous téléchargeons le fichier .dta et le renommons
  assign(objname, read_dta(dta_file), envir = .GlobalEnv)
}

# ==============================================================================
# 1. DÉFINITION DES FONCTIONS D'AGRÉGATION (AU NIVEAU MENSUEL)
# ==============================================================================

# A. Fonction générique : Agrégation Mensuelle (retourne colonnes _1 à _12)
aggregate_monthly <- function(data, date_col, exp_col, prefix) {
  date_col_sym <- sym(date_col)
  exp_col_sym <- sym(exp_col)
  total_col_name <- paste0(prefix, "total")
  
  result <- data %>%
    group_by(DUPERSID, month = !!date_col_sym) %>%
    summarise(exp_month = sum(!!exp_col_sym, na.rm = TRUE), .groups = "drop") %>%
    # Pivot pour avoir une colonne par mois (1 à 12)
    pivot_wider(
      names_from = month,
      values_from = exp_month,
      names_prefix = paste0(prefix, ""), # donnera prefix_1, prefix_2...
      values_fill = 0
    )
  
  # On s'assure que toutes les colonnes mois (1-12) existent, même si pas de données
  for(m in 1:12) {
    col_name <- paste0(prefix, m)
    if(!col_name %in% names(result)) {
      result[[col_name]] <- 0
    }
  }
  
  # Calcul du total annuel pour vérification
  result <- result %>%
    rowwise() %>%
    mutate(!!total_col_name := sum(c_across(matches(paste0("^", prefix, "[0-9]+$"))), na.rm = TRUE)) %>%
    ungroup()
  
  return(result)
}

# B. Fonction Uniforme : Répartition sur 12 mois
aggregate_uniform_monthly <- function(data, exp_col, prefix) {
  exp_col_sym <- sym(exp_col)
  total_col_name <- paste0(prefix, "total")
  
  monthly_data <- data %>%
    mutate(exp_monthly_val = !!exp_col_sym / 12) %>%
    tidyr::crossing(month = 1:12) %>%
    group_by(DUPERSID, month) %>%
    summarise(exp_final = sum(exp_monthly_val, na.rm = TRUE), .groups = "drop") %>%
    pivot_wider(
      names_from = month,
      values_from = exp_final,
      names_prefix = paste0(prefix, ""),
      values_fill = 0
    ) %>%
    rowwise() %>%
    mutate(!!total_col_name := sum(c_across(matches(paste0("^", prefix, "[0-9]+$"))), na.rm = TRUE)) %>%
    ungroup()
  
  return(monthly_data)
}

# ==============================================================================
# 2. TRAITEMENT DES FICHIERS (GENERATION MENSUELLE)
# ==============================================================================

# --- Traitement Standard ---
agg_dental <- aggregate_monthly(dental_2022, "DVDATEMM", "DVXP22X", "exp_dental_")
agg_outpatient <- aggregate_monthly(outpatients_visits_2022, "OPDATEMM", "OPXP22X", "exp_outpatient_")
agg_office <- aggregate_monthly(Office_Based_Medical_Provider_2022, "OBDATEMM", "OBXP22X", "exp_office_")
agg_er <- aggregate_monthly(emergency_room_2022, "ERDATEMM", "ERXP22X", "exp_er_")
agg_home <- aggregate_monthly(home_health_2022, "HHDATEMM", "HHXP22X", "exp_home_")

# --- Traitement Uniforme ---
agg_medicines <- aggregate_uniform_monthly(prescribed_medicines_2022, "RXXP22X", "exp_medicines_")
agg_others <- aggregate_uniform_monthly(others_2022, "OMXP22X", "exp_others_")

# --- Traitement Hospital (Spécial) ---
# On reprend votre logique mensuelle intacte
hospitals_monthly <- hospitals_2022 %>%
  filter(IPENDMM != -8) %>%
  mutate(
    n_months = case_when(IPBEGYR == 2022 ~ IPENDMM - IPBEGMM + 1, IPBEGYR == 2021 ~ (12 - IPBEGMM + 1) + IPENDMM, TRUE ~ NA_real_),
    exp_hospitals_month = IPXP22X / n_months
  ) %>%
  rowwise() %>%
  mutate(month = case_when(IPBEGYR == 2022 ~ list(seq(IPBEGMM, IPENDMM)), IPBEGYR == 2021 ~ list(seq(1, IPENDMM)), TRUE ~ list(NA_integer_))) %>%
  unnest(month) %>%
  ungroup() %>%
  group_by(DUPERSID, month) %>%
  summarise(exp_hospitals = sum(exp_hospitals_month), .groups = "drop") %>%
  complete(DUPERSID, month = 1:12, fill = list(exp_hospitals = 0))

# Gestion Hospital 2021 pour cohérence
exp_2021 <- hospitals_2022 %>%
  filter(IPENDMM != -8, IPBEGYR == 2021) %>%
  mutate(n_months = (12 - IPBEGMM + 1) + IPENDMM, exp_hospitals_2021 = (IPXP22X / n_months) * (12 - IPBEGMM + 1)) %>%
  group_by(DUPERSID) %>%
  summarise(exp_hospitals_2021_total = sum(exp_hospitals_2021))

agg_hospital <- hospitals_monthly %>%
  pivot_wider(names_from = month, values_from = exp_hospitals, names_prefix = "exp_hospitals_", values_fill = 0) %>%
  rowwise() %>%
  mutate(exp_hospitals_2022_total = sum(c_across(matches("^exp_hospitals_[0-9]+$")), na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(exp_2021, by = "DUPERSID")

# ==============================================================================
# 3. JOINTURES ET NETTOYAGE
# ==============================================================================

ids_removed <- hospitals_2022 %>% filter(IPENDMM == -8 ) %>% pull(DUPERSID)

all_expenses <- full_year_2022 %>%
  filter(!DUPERSID %in% ids_removed)  %>%
  select(DUPERSID,  EDUCYR,  TTLP22X, FAMINC22, AGE22X, TOTEXP22 ) %>%
  left_join(full_year_2023 %>% select(DUPERSID,  TOTEXP23), by= "DUPERSID") %>%
  left_join(longitudinal %>% select(DUPERSID), by= "DUPERSID") %>%
  left_join(agg_dental, by="DUPERSID") %>%
  left_join(agg_hospital, by="DUPERSID") %>%
  left_join(agg_outpatient, by="DUPERSID") %>%
  left_join(agg_office, by="DUPERSID") %>%
  left_join(agg_er, by="DUPERSID") %>%
  left_join(agg_home, by="DUPERSID") %>%
  left_join(agg_others, by="DUPERSID") %>%
  left_join(agg_medicines, by="DUPERSID")

# Nettoyage NA et Négatifs
all_expenses_clean <- all_expenses %>% 
  mutate(EDUCYR = ifelse(EDUCYR < 0, 0, EDUCYR)) %>%
  filter(TTLP22X >=0 & FAMINC22 >=0 & AGE22X >=0)

all_expense_cols <- names(all_expenses_clean) %>% str_subset("^exp_")
home_expense_cols <- names(all_expenses_clean) %>% str_subset("^exp_home")

all_expenses_clean <- all_expenses_clean %>%
  mutate(across(all_of(all_expense_cols), ~ replace_na(.x, 0))) %>% 
  mutate(TOTEXP23 = replace_na(TOTEXP23, 0)) %>% 
  mutate(across(all_of(home_expense_cols), ~ pmax(.x, 0)))

# ==============================================================================
# 4. CALCUL DES INDICATEURS MENSUELS (TENDANCE, PIC)
# ==============================================================================

# 1. Création des variables dépenses totales mensuelles (Globales)
# On somme toutes les catégories (dental_1 + hospital_1 + ...) pour avoir dep_janvier
all_expenses_clean <- all_expenses_clean %>%
  rowwise() %>%
  mutate(
    dep_1  = sum(c_across(ends_with("_1")), na.rm = TRUE),
    dep_2  = sum(c_across(ends_with("_2")), na.rm = TRUE),
    dep_3  = sum(c_across(ends_with("_3")), na.rm = TRUE),
    dep_4  = sum(c_across(ends_with("_4")), na.rm = TRUE),
    dep_5  = sum(c_across(ends_with("_5")), na.rm = TRUE),
    dep_6  = sum(c_across(ends_with("_6")), na.rm = TRUE),
    dep_7  = sum(c_across(ends_with("_7")), na.rm = TRUE),
    dep_8  = sum(c_across(ends_with("_8")), na.rm = TRUE),
    dep_9  = sum(c_across(ends_with("_9")), na.rm = TRUE),
    dep_10 = sum(c_across(ends_with("_10")), na.rm = TRUE),
    dep_11 = sum(c_across(ends_with("_11")), na.rm = TRUE),
    dep_12 = sum(c_across(ends_with("_12")), na.rm = TRUE)
  ) %>%
  ungroup()

# Liste des mois pour les itérations
monthly_cols <- paste0("dep_", 1:12)

# 2. Calcul des indicateurs complexes
all_expenses_clean <- all_expenses_clean %>%
  rowwise() %>%
  mutate(
    # A. Nombre de mois au-dessus de la moyenne mensuelle
    nbre_au_dessus_moyenne = sum(c_across(all_of(monthly_cols)) > (TOTEXP22 / 12), na.rm = TRUE),
    
    # B. Tendance sur les 3 derniers mois (Oct, Nov, Dec => 1, 2, 3)
    tendance = {
      mois_q4 <- 1:3
      vals_q4 <- c(dep_10, dep_11, dep_12)
      if(all(vals_q4 == 0)) 0 else lm(vals_q4 ~ mois_q4)$coefficients[2]
    },
    
    # C. Épisode Aigu (Basé sur le Max mensuel)
    dep_max_mensuel = max(c_across(all_of(monthly_cols)), na.rm = TRUE),
    ep_aigue = if_else(dep_max_mensuel > 5 * (TOTEXP22 / 12), 1, 0)
  ) %>%
  ungroup()

# ==============================================================================
# 5. AGRÉGATION FINALE PAR SAISON (POUR RÉDUIRE LA DIMENSION)
# ==============================================================================
# Maintenant que les indicateurs fins sont calculés, on regroupe en saisons
# Hiver: 1,2,3 | Printemps: 4,5,6 | Eté: 7,8,9 | Automne: 10,11,12

all_expenses_clean <- all_expenses_clean %>%
  mutate(
    dep_winter = dep_1 + dep_2 + dep_3,
    dep_spring = dep_4 + dep_5 + dep_6,
    dep_summer = dep_7 + dep_8 + dep_9,
    dep_fall   = dep_10 + dep_11 + dep_12,
    
    # Dépense 6 derniers mois (Eté + Automne)
    dep_last_6_months = dep_summer + dep_fall
  ) %>%
  # NETTOYAGE : On supprime les colonnes mensuelles intermédiaires pour alléger
  select(
    -matches("^dep_[0-9]+$"), # Supprime dep_1 à dep_12
    -matches("_(1|2|3|4|5|6|7|8|9|10|11|12)$") # Supprime les colonnes sources exp_..._1
  )

# Affichage résultat
print(head(all_expenses_clean %>% select(starts_with("dep_"), tendance, ep_aigue)))






write.csv(all_expenses_clean, "saison/all_expenses_saison.csv", row.names = FALSE)
