import re
from pymongo import MongoClient
import datetime

client = MongoClient("mongodb://localhost:27017/")
db = client["infinityproduct_dev"]

def build_facet_bag_for_klass(klass_name: str, obs_list: list) -> dict:
    """
    Constructs a clean, high-density FacetBag describing the observed configuration space
    from raw compost observations. Filters out SKU/vendor noise.
    """
    facet_bag = {}
    
    # 1. Sizes (Natural sorted)
    raw_sizes = set()
    for o in obs_list:
        sz = o.get("attributes", {}).get("size")
        if sz and str(sz).lower() not in ["none", "n/a", "null"]:
            raw_sizes.add(str(sz).strip())
            
    if raw_sizes:
        # Sort numerically if possible, else alphabetically
        def size_sort_key(s):
            m = re.search(r"[-+]?\d*\.\d+|\d+", s)
            return (float(m.group(0)) if m else 9999, s)
        facet_bag["Size"] = sorted(list(raw_sizes), key=size_sort_key)
        
    # 2. Materials
    raw_mats = set()
    for o in obs_list:
        for m in o.get("materials", []):
            if m and str(m).lower() not in ["none", "n/a", "not explicitly stated"]:
                raw_mats.add(m.strip())
    if raw_mats:
        facet_bag["Material"] = sorted(list(raw_mats))
        
    # 3. Dynamic Structural Facets inferred from product titles & attributes
    # e.g., Cuff: [Cuffed, Uncuffed], Construction: [Reinforced, Standard], etc.
    cuff_types = set()
    constructions = set()
    connectors = set()
    valves = set()
    lumens = set()
    
    for o in obs_list:
        full_text = f"{o.get('product_name', '')} {' '.join([f'{k}:{v}' for k, v in o.get('attributes', {}).items()])}".lower()
        
        # Cuff
        if "uncuffed" in full_text or "without cuff" in full_text:
            cuff_types.add("Uncuffed")
        elif "cuffed" in full_text or "with cuff" in full_text or "cuff (" in full_text:
            cuff_types.add("Cuffed (HVLP)")
            
        # Construction / Reinforcement
        if "reinforced" in full_text or "spiral" in full_text:
            constructions.add("Spiral Reinforced")
        elif "preformed" in full_text:
            constructions.add("Preformed")
        elif "standard" in full_text or "straight" in full_text:
            constructions.add("Standard")
            
        # Connectors / Ports
        for k, v in o.get("attributes", {}).items():
            k_lower = k.lower()
            if "connector" in k_lower or "port" in k_lower or "hub" in k_lower:
                if str(v).lower() not in ["none", "n/a"]:
                    connectors.add(str(v).strip())
            elif "valve" in k_lower:
                valves.add(str(v).strip())
            elif "lumen" in k_lower or "way" in k_lower:
                lumens.add(str(v).strip())
                
    if cuff_types:
        facet_bag["Cuff"] = sorted(list(cuff_types))
    if len(constructions) > 1 or (constructions and "Spiral Reinforced" in constructions):
        facet_bag["Construction"] = sorted(list(constructions))
    if connectors:
        facet_bag["Connector"] = sorted(list(connectors))
    if valves:
        facet_bag["Valve"] = sorted(list(valves))
    if lumens:
        facet_bag["Lumen / Ways"] = sorted(list(lumens))
        
    return facet_bag

def synthesize_all_klasses():
    """Generates and persists FacetBag artifacts on all Klass documents."""
    klasses = list(db.klasses.find({}))
    now = datetime.datetime.now(datetime.timezone.utc)
    updated = 0
    
    for k in klasses:
        k_name = k["name"]
        slug = k["slug"]
        obs_list = list(db.observations.find({"klass_name": {"$regex": f"^{k_name}$", "$options": "i"}}))
        if not obs_list:
            continue
            
        bag = build_facet_bag_for_klass(k_name, obs_list)
        
        db.klasses.update_one(
            {"_id": k["_id"]},
            {
                "$set": {
                    "facet_bag": bag,
                    "facet_bag_version": 1,
                    "facet_bag_generated_at": now,
                    "total_observations": len(obs_list)
                }
            }
        )
        updated += 1
        
    print(f"[OK] Synthesized clean FacetBags for {updated} Klasses!")

if __name__ == "__main__":
    synthesize_all_klasses()
    # Test Tracheostomy Tube
    trach = db.klasses.find_one({"slug": "tracheostomy-tube"})
    print("\n--- Tracheostomy Tube FacetBag ---")
    import pprint
    pprint.pprint(trach.get("facet_bag"))
