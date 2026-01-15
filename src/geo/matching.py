import unicodedata
import pandas as pd
import logging
import os
from rapidfuzz import process, fuzz

logger = logging.getLogger("geo_matching")

class GeoMatcher:
    def __init__(self, catalog_path: str = "assets/geo/territory_catalog.csv"):
        self.catalog_path = catalog_path
        self.catalog = None
        self.provinces = []
        self.cantons_by_prov = {}
        self.valid_pairs = set()
        
        self.load_catalog()

    def normalize_text(self, text: str) -> str:
        if not isinstance(text, str):
            return ""
        # 1. Normalize unicode (remove accents)
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
        # 2. Lowercase and trim
        text = text.lower().strip()
        # 3. Collapse multiple spaces
        text = " ".join(text.split())
        return text

    def load_catalog(self):
        if not os.path.exists(self.catalog_path):
            logger.warning(f"Territory catalog not found at {self.catalog_path}. Matching will fail.")
            return
        
        try:
            df = pd.read_csv(self.catalog_path)
            # Ensure columns exist
            required = ['provincia_norm', 'canton_norm']
            if not all(col in df.columns for col in required):
                # Try to normalize on the fly if original cols exist
                if 'provincia' in df.columns and 'canton' in df.columns:
                    df['provincia_norm'] = df['provincia'].apply(self.normalize_text)
                    df['canton_norm'] = df['canton'].apply(self.normalize_text)
                else:
                    logger.error("Catalog missing required columns.")
                    return

            self.catalog = df
            self.provinces = df['provincia_norm'].unique().tolist()
            self.valid_pairs = set(zip(df['provincia_norm'], df['canton_norm']))
            
            # Build lookups
            for _, row in df.iterrows():
                p = row['provincia_norm']
                c = row['canton_norm']
                if p not in self.cantons_by_prov:
                    self.cantons_by_prov[p] = []
                self.cantons_by_prov[p].append(c)
                
            logger.info(f"Loaded territory catalog: {len(self.provinces)} provinces.")
        except Exception as e:
            logger.error(f"Failed to load catalog: {e}")

    def match_territory(self, provincia_input: str, canton_input: str, threshold: int = 85):
        """
        Returns (provincia_norm, canton_norm, score_prov, score_canton, method).
        Method: 'exact', 'fuzzy', or 'failed'.
        """
        if self.catalog is None:
            return None, None, 0, 0, 'no_catalog'

        prov_norm_input = self.normalize_text(provincia_input)
        canton_norm_input = self.normalize_text(canton_input)

        # 1. Exact Match Check
        # Check if province exists
        matched_prov = None
        prov_score = 100
        
        if prov_norm_input in self.provinces:
            matched_prov = prov_norm_input
        else:
            # Fuzzy match province
            best_prov = process.extractOne(prov_norm_input, self.provinces, scorer=fuzz.ratio)
            if best_prov and best_prov[1] >= threshold:
                matched_prov = best_prov[0]
                prov_score = best_prov[1]
            else:
                return None, None, 0, 0, 'failed_prov'

        # 2. Canton Match Check (scoped to province)
        valid_cantons = self.cantons_by_prov.get(matched_prov, [])
        matched_canton = None
        canton_score = 100

        if canton_norm_input in valid_cantons:
            matched_canton = canton_norm_input
        else:
             # Fuzzy match canton
            best_canton = process.extractOne(canton_norm_input, valid_cantons, scorer=fuzz.ratio)
            if best_canton and best_canton[1] >= threshold:
                matched_canton = best_canton[0]
                canton_score = best_canton[1]
            else:
                return matched_prov, None, prov_score, 0, 'failed_canton'

        method = 'exact' if prov_score == 100 and canton_score == 100 else 'fuzzy'
        return matched_prov, matched_canton, prov_score, canton_score, method

    def is_valid_pair(self, provincia_norm: str, canton_norm: str) -> bool:
        if not provincia_norm or not canton_norm:
            return False
        return (provincia_norm, canton_norm) in self.valid_pairs
