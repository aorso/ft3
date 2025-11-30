import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from pathlib import Path
import warnings

# Imports optionnels pour l'API (si non disponibles, on utilisera uniquement le Excel)
try:
    import qis.cj_tools as cj
    API_AVAILABLE = True
except ImportError:
    API_AVAILABLE = False
    cj = None

try:
    from cpypdl2 import pypdl
    PYPDL_AVAILABLE = True
except ImportError:
    PYPDL_AVAILABLE = False
    pypdl = None

try:
    from tqdm.notebook import tqdm
except ImportError:
    # Si tqdm n'est pas disponible, on définit une fonction dummy
    def tqdm(iterable, *args, **kwargs):
        return iterable


class CurrencyConverter:
    """
    Classe pour convertir des montants entre devises avec gestion automatique
    API (prioritaire) et Excel (secours).
    Structure Excel: Date + une colonne par paire de devises (EUR_USD, EUR_NOK, etc.)
    """
    
    def __init__(self, excel_backup_path=None):
        """
        Initialise le convertisseur.
        
        Parameters:
        -----------
        excel_backup_path : str, optional
            Chemin vers le fichier Excel de secours. Par défaut: 'data/taux_change_backup.xlsx'
        """
        # Définir le chemin par défaut dans le dossier data/
        if excel_backup_path is None:
            excel_backup_path = 'data/taux_change_backup.xlsx'
        
        # Créer le dossier data/ s'il n'existe pas
        data_dir = Path(excel_backup_path).parent
        if data_dir != Path('.'):
            data_dir.mkdir(parents=True, exist_ok=True)
        
        self.excel_backup_path = excel_backup_path
        self.rates_df = pd.DataFrame()  # DataFrame avec Date comme colonne + colonnes par paire
        self.devise_pivot = 'EUR'
        self.last_update_date = None
        
        # Afficher le statut de l'API
        if API_AVAILABLE and PYPDL_AVAILABLE:
            print("✓ API disponible - utilisation de l'API en priorité, Excel en secours")
        else:
            print("ℹ API non disponible - utilisation uniquement du Excel de secours")
        
        # Charger le Excel de secours s'il existe
        self._load_backup_excel()
    
    def _normalize_pair(self, pair):
        """
        Normalise une paire de devises (uppercase, strip).
        
        Parameters:
        -----------
        pair : str
            Paire de devises (ex: "EUR_USD" ou "eur_usd")
            
        Returns:
        --------
        str
            Paire normalisée
        """
        return str(pair).upper().strip()
    
    def _get_inverted_pair(self, pair):
        """
        Retourne la paire inversée.
        
        Parameters:
        -----------
        pair : str
            Paire de devises (ex: "EUR_USD")
            
        Returns:
        --------
        str ou None
            Paire inversée (ex: "USD_EUR") ou None si format invalide
        """
        parts = pair.split('_')
        if len(parts) == 2:
            return f"{parts[1]}_{parts[0]}"
        return None
    
    def _load_backup_excel(self):
        """Charge le fichier Excel de secours s'il existe."""
        if Path(self.excel_backup_path).exists():
            try:
                df = pd.read_excel(self.excel_backup_path)
                df.columns = df.columns.str.strip().str.upper()
                
                # Vérifier que DATE existe
                if 'DATE' not in df.columns:
                    # Afficher uniquement si l'API n'est pas disponible
                    if not (API_AVAILABLE and PYPDL_AVAILABLE):
                        print("⚠ Colonne DATE manquante dans l'Excel")
                    self.rates_df = pd.DataFrame()
                    return
                
                # Normaliser la colonne DATE
                df['DATE'] = pd.to_datetime(df['DATE']).dt.date
                
                # Supprimer les colonnes non-numériques sauf DATE
                rate_columns = [col for col in df.columns if col != 'DATE' and col != 'LAST_UPDATE']
                
                # Convertir les colonnes de taux en numérique
                for col in rate_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Supprimer les lignes où toutes les valeurs de taux sont NaN
                df = df.dropna(subset=rate_columns, how='all')
                
                # Trier par date
                df = df.sort_values('DATE')
                
                self.rates_df = df
                
                # Vérifier la date de dernière mise à jour
                if 'LAST_UPDATE' in df.columns:
                    last_update = df['LAST_UPDATE'].dropna()
                    if not last_update.empty:
                        self.last_update_date = pd.to_datetime(last_update.iloc[0]).date()
                else:
                    # Utiliser la date de modification du fichier
                    file_time = Path(self.excel_backup_path).stat().st_mtime
                    self.last_update_date = datetime.fromtimestamp(file_time).date()
                
                # Afficher uniquement si l'API n'est pas disponible
                if not (API_AVAILABLE and PYPDL_AVAILABLE):
                    print(f"✓ Excel de secours chargé: {len(df)} dates, {len(rate_columns)} paires disponibles")
            except Exception as e:
                # Afficher les erreurs uniquement si l'API n'est pas disponible
                if not (API_AVAILABLE and PYPDL_AVAILABLE):
                    print(f"⚠ Erreur lors du chargement de l'Excel: {e}")
                self.rates_df = pd.DataFrame()
        else:
            # Afficher uniquement si l'API n'est pas disponible
            if not (API_AVAILABLE and PYPDL_AVAILABLE):
                print("ℹ Aucun fichier Excel de secours trouvé")
    
    def _should_update_backup(self):
        """
        Vérifie si le backup Excel doit être mis à jour (toutes les semaines).
        
        Returns:
        --------
        bool
            True si la mise à jour est nécessaire
        """
        if self.last_update_date is None:
            return True
        
        days_since_update = (date.today() - self.last_update_date).days
        return days_since_update >= 7
    
    def _try_load_from_api(self, pairs, start_date, end_date):
        """
        Essaie de charger les taux depuis l'API.
        
        Returns:
        --------
        pd.DataFrame ou None
            DataFrame avec Date + colonnes par paire, ou None si l'API échoue
        """
        # Vérifier si l'API est disponible
        if not API_AVAILABLE or not PYPDL_AVAILABLE:
            # Pas besoin de message d'erreur, c'est normal si l'API n'est pas disponible
            return None
        
        try:
            # Convertir les dates
            if isinstance(start_date, date):
                start_date_str = start_date.strftime("%Y%m%d")
            else:
                start_date_str = str(start_date)
            
            if isinstance(end_date, date):
                end_date_str = end_date.strftime("%Y%m%d")
            else:
                end_date_str = str(end_date)
            
            # Appel API
            df_api = cj.get_ts_list_2(
                pairs,
                ["mds_default"] * len(pairs),
                pypdl.to_date(start_date_str),
                pypdl.to_date(end_date_str)
            )
            
            # Transformer en format interne (Date + colonnes par paire)
            if df_api.empty:
                return None
            
            # L'API retourne un DataFrame avec la date en index
            # Réinitialiser l'index pour avoir Date comme colonne
            df_result = df_api.reset_index()
            
            # La première colonne après reset_index est généralement la date
            # Vérifier si c'est bien une date
            date_col = df_result.columns[0]
            try:
                df_result['DATE'] = pd.to_datetime(df_result[date_col]).dt.date
            except:
                # Si la première colonne n'est pas une date, utiliser l'index
                df_result['DATE'] = pd.to_datetime(df_api.index).date()
            
            # Garder seulement DATE + les colonnes de paires
            columns_to_keep = ['DATE'] + [col for col in df_result.columns if col in pairs]
            df_result = df_result[columns_to_keep].copy()
            
            # Convertir les taux en float
            for pair in pairs:
                if pair in df_result.columns:
                    df_result[pair] = pd.to_numeric(df_result[pair], errors='coerce')
            
            # Supprimer les lignes où toutes les valeurs sont NaN
            df_result = df_result.dropna(subset=[col for col in df_result.columns if col != 'DATE'], how='all')
            
            if len(df_result) > 0:
                return df_result
            return None
            
        except Exception as e:
            print(f"⚠ API non disponible: {e}")
            return None
    
    def _update_backup_excel(self):
        """Met à jour le fichier Excel de secours avec les données actuelles."""
        if self.rates_df.empty:
            return
        
        try:
            df_to_save = self.rates_df.copy()
            
            # Ajouter la date de mise à jour dans une colonne séparée (première ligne seulement)
            if 'LAST_UPDATE' not in df_to_save.columns:
                df_to_save['LAST_UPDATE'] = None
            df_to_save.loc[df_to_save.index[0], 'LAST_UPDATE'] = date.today()
            
            # Réorganiser les colonnes: DATE, LAST_UPDATE, puis les paires
            cols = ['DATE', 'LAST_UPDATE'] + [col for col in df_to_save.columns if col not in ['DATE', 'LAST_UPDATE']]
            df_to_save = df_to_save[cols]
            
            df_to_save.to_excel(self.excel_backup_path, index=False)
            self.last_update_date = date.today()
            print(f"✓ Excel de secours mis à jour: {len(df_to_save)} dates sauvegardées")
        except Exception as e:
            print(f"⚠ Erreur lors de la sauvegarde Excel: {e}")
    
    def _merge_rates_dataframe(self, new_df):
        """
        Fusionne un nouveau DataFrame de taux avec le DataFrame existant.
        
        Parameters:
        -----------
        new_df : pd.DataFrame
            Nouveau DataFrame avec Date + colonnes par paire
        """
        if self.rates_df.empty:
            self.rates_df = new_df.copy()
        else:
            # Fusionner sur DATE
            self.rates_df = pd.merge(
                self.rates_df,
                new_df,
                on='DATE',
                how='outer',
                suffixes=('', '_new')
            )
            
            # Pour les colonnes en double, prendre les nouvelles valeurs
            for col in new_df.columns:
                if col != 'DATE':
                    if f"{col}_new" in self.rates_df.columns:
                        # Remplacer les valeurs manquantes par les nouvelles
                        self.rates_df[col] = self.rates_df[col].fillna(self.rates_df[f"{col}_new"])
                        self.rates_df = self.rates_df.drop(columns=[f"{col}_new"])
                    elif col not in self.rates_df.columns:
                        self.rates_df[col] = new_df[col]
            
            # Trier par date
            self.rates_df = self.rates_df.sort_values('DATE').reset_index(drop=True)
    
    def import_rates(self, pairs, start_date=None, end_date=None, target_date=None):
        """
        Importe un ou plusieurs taux de change pour une date ou une plage de dates.
        Retourne un DataFrame avec les taux.
        
        Parameters:
        -----------
        pairs : str ou list
            Paire(s) de devises (ex: "EUR_USD" ou ["EUR_USD", "USD_EUR"])
        start_date : date, str ou None
            Date de début (format YYYY-MM-DD ou YYYYMMDD). Requis si end_date est fourni.
        end_date : date, str ou None
            Date de fin (format YYYY-MM-DD ou YYYYMMDD). Requis si start_date est fourni.
        target_date : date, str ou None
            Date unique (format YYYY-MM-DD ou YYYYMMDD). Alternative à start_date/end_date.
            
        Returns:
        --------
        pd.DataFrame
            DataFrame avec colonnes: Date + une colonne par paire
        """
        # Normaliser les paires
        if isinstance(pairs, str):
            pairs = [pairs]
        pairs = [self._normalize_pair(p) for p in pairs]
        
        # Normaliser les dates
        if target_date:
            if isinstance(target_date, str):
                target_date = pd.to_datetime(target_date).date()
            elif isinstance(target_date, datetime):
                target_date = target_date.date()
            start_date = target_date
            end_date = target_date
        else:
            if isinstance(start_date, str):
                start_date = pd.to_datetime(start_date).date()
            elif isinstance(start_date, datetime):
                start_date = start_date.date()
            
            if isinstance(end_date, str):
                end_date = pd.to_datetime(end_date).date()
            elif isinstance(end_date, datetime):
                end_date = end_date.date()
        
        if not start_date or not end_date:
            raise ValueError("Vous devez fournir soit target_date, soit start_date et end_date")
        
        # Essayer d'abord l'API
        df_api = self._try_load_from_api(pairs, start_date, end_date)
        
        if df_api is not None:
            # API fonctionne: fusionner avec le DataFrame interne
            self._merge_rates_dataframe(df_api)
            
            # Mettre à jour le backup Excel si nécessaire
            if self._should_update_backup():
                self._update_backup_excel()
            
            return df_api
        else:
            # API non disponible ou échoue: utiliser le Excel de secours
            if self.rates_df.empty:
                print(f"⚠ Aucune donnée disponible dans le backup")
                return pd.DataFrame()
            
            # Filtrer par date
            mask = (self.rates_df['DATE'] >= start_date) & (self.rates_df['DATE'] <= end_date)
            df_backup = self.rates_df[mask].copy()
            
            if df_backup.empty:
                print(f"⚠ Aucun taux trouvé dans le backup entre {start_date} et {end_date}")
                return pd.DataFrame()
            
            # Sélectionner seulement les colonnes demandées (directes ou inversées)
            columns_to_return = ['DATE']
            for pair in pairs:
                if pair in df_backup.columns:
                    columns_to_return.append(pair)
                else:
                    # Chercher la paire inversée
                    inverted_pair = self._get_inverted_pair(pair)
                    if inverted_pair and inverted_pair in df_backup.columns:
                        columns_to_return.append(inverted_pair)
                        # Créer la colonne avec le taux inversé
                        df_backup[pair] = 1.0 / df_backup[inverted_pair]
            
            result_df = df_backup[columns_to_return].copy()
            
            if result_df.empty:
                print(f"⚠ Aucun taux trouvé pour {pairs} dans le backup")
            
            return result_df
    
    def _find_closest_rate(self, pair, target_date):
        """
        Trouve le taux de change le plus proche pour une paire et une date.
        Gère automatiquement les paires inversées.
        Retourne toujours le taux dans le sens demandé (ex: si on demande EUR_USD, 
        retourne toujours combien d'USD pour 1 EUR).
        
        Parameters:
        -----------
        pair : str
            Paire de devises (ex: "EUR_USD" signifie 1 EUR = X USD)
        target_date : date
            Date cible
            
        Returns:
        --------
        float ou None
            Taux de change le plus proche dans le sens demandé
        """
        if self.rates_df.empty:
            return None
        
        pair = self._normalize_pair(pair)
        
        # Chercher d'abord la paire directe
        if pair in self.rates_df.columns:
            # Filtrer par date et trouver la plus proche
            df_filtered = self.rates_df[self.rates_df['DATE'].notna()].copy()
            df_filtered['DAYS_DIFF'] = (df_filtered['DATE'] - target_date).abs()
            
            if df_filtered.empty:
                return None
            
            closest_idx = df_filtered['DAYS_DIFF'].idxmin()
            rate = df_filtered.loc[closest_idx, pair]
            
            if pd.notna(rate):
                return float(rate)
        
        # Si pas trouvé, essayer la paire inversée
        inverted_pair = self._get_inverted_pair(pair)
        if inverted_pair and inverted_pair in self.rates_df.columns:
            df_filtered = self.rates_df[self.rates_df['DATE'].notna()].copy()
            df_filtered['DAYS_DIFF'] = (df_filtered['DATE'] - target_date).abs()
            
            if df_filtered.empty:
                return None
            
            closest_idx = df_filtered['DAYS_DIFF'].idxmin()
            rate = df_filtered.loc[closest_idx, inverted_pair]
            
            if pd.notna(rate):
                # Retourner l'inverse du taux pour avoir le taux dans le sens demandé
                # Ex: si on cherche EUR_USD mais qu'on trouve USD_EUR = 0.9, alors EUR_USD = 1/0.9 = 1.11
                return 1.0 / float(rate)
        
        return None
    
    def _get_rate_via_eur(self, from_currency, to_currency, target_date):
        """
        Obtient le taux de change entre deux devises via EUR comme pivot.
        Gère automatiquement les paires inversées.
        La fonction _find_closest_rate gère déjà les inversions, donc on peut simplifier.
        
        Parameters:
        -----------
        from_currency : str
            Devise source
        to_currency : str
            Devise cible
        target_date : date
            Date du taux
            
        Returns:
        --------
        float ou None
            Taux de change calculé (taux pour convertir 1 unité de from_currency en to_currency)
        """
        from_currency = from_currency.upper()
        to_currency = to_currency.upper()
        
        # Cas 1: Conversion vers EUR
        if to_currency == self.devise_pivot:
            # Chercher EUR/from_currency (1 EUR = X from, donc 1 from = 1/X EUR)
            # _find_closest_rate gère déjà les inversions
            pair = f"{self.devise_pivot}_{from_currency}"
            rate = self._find_closest_rate(pair, target_date)
            if rate:
                # Si on a trouvé EUR/from, alors rate = EUR/from, donc 1 from = 1/rate EUR
                # Si on a trouvé from/EUR, alors _find_closest_rate a déjà retourné l'inverse
                # On doit vérifier quel format on a
                # Pour simplifier, on suppose toujours EUR/XXX dans notre stockage
                # Donc si on trouve EUR/from, on retourne 1/rate
                return 1.0 / rate
            return None
        
        # Cas 2: Conversion depuis EUR
        if from_currency == self.devise_pivot:
            # Chercher EUR/to_currency (1 EUR = X to)
            pair = f"{self.devise_pivot}_{to_currency}"
            rate = self._find_closest_rate(pair, target_date)
            return rate
        
        # Cas 3: Conversion entre deux devises non-EUR
        # Exemple: NOK -> USD via EUR
        # On cherche: EUR/NOK et EUR/USD
        # Conversion: NOK -> EUR -> USD
        # 1 NOK = 1/(EUR/NOK) EUR = (1/(EUR/NOK)) * (EUR/USD) USD
        
        pair_eur_from = f"{self.devise_pivot}_{from_currency}"
        rate_eur_from = self._find_closest_rate(pair_eur_from, target_date)
        
        pair_eur_to = f"{self.devise_pivot}_{to_currency}"
        rate_eur_to = self._find_closest_rate(pair_eur_to, target_date)
        
        if rate_eur_from and rate_eur_to:
            # rate_eur_from = EUR/from (ou l'inverse si trouvé via paire inversée)
            # rate_eur_to = EUR/to (ou l'inverse si trouvé via paire inversée)
            # _find_closest_rate retourne toujours le taux dans le sens demandé
            # Donc si on demande EUR/XXX, on obtient toujours EUR/XXX
            
            # 1 from = 1/(EUR/from) EUR = (1/rate_eur_from) EUR
            # 1 EUR = rate_eur_to to
            # Donc: 1 from = (1/rate_eur_from) * rate_eur_to to
            return (1.0 / rate_eur_from) * rate_eur_to
        
        return None
    
    def convert(self, amount, from_currency, to_currency, target_date):
        """
        Convertit un montant d'une devise à une autre pour une date donnée.
        
        Parameters:
        -----------
        amount : float
            Montant à convertir
        from_currency : str
            Devise source (ex: "NOK")
        to_currency : str
            Devise cible (ex: "USD")
        target_date : date ou str
            Date du taux de change (format YYYY-MM-DD ou YYYYMMDD)
            
        Returns:
        --------
        float ou None
            Montant converti, ou None si le taux n'est pas disponible
        """
        # Normaliser la date
        if isinstance(target_date, str):
            target_date = pd.to_datetime(target_date).date()
        elif isinstance(target_date, datetime):
            target_date = target_date.date()
        
        # Si même devise
        if from_currency.upper() == to_currency.upper():
            return amount
        
        # Essayer d'abord de récupérer le taux depuis les données en mémoire
        rate = self._get_rate_via_eur(from_currency, to_currency, target_date)
        
        # Si pas trouvé, essayer d'importer depuis l'API/Excel
        if rate is None:
            # Déterminer les paires nécessaires (on essaie toujours avec EUR comme base)
            pairs_needed = []
            if from_currency.upper() != self.devise_pivot:
                pairs_needed.append(f"{self.devise_pivot}_{from_currency.upper()}")
            if to_currency.upper() != self.devise_pivot:
                pairs_needed.append(f"{self.devise_pivot}_{to_currency.upper()}")
            
            # Importer les taux manquants
            self.import_rates(pairs_needed, target_date=target_date)
            
            # Réessayer d'obtenir le taux
            rate = self._get_rate_via_eur(from_currency, to_currency, target_date)
        
        if rate is None:
            print(f"⚠ Aucun taux de change trouvé pour {from_currency} -> {to_currency} à la date {target_date}")
            return None
        
        return amount * rate
