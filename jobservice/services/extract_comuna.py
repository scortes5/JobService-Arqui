import re
from typing import Optional

COMUNAS = {
    # Región de Arica y Parinacota
    "Arica","Camarones","General Lagos","Putre",
    # Región de Tarapacá
    "Alto Hospicio","Camiña","Colchane","Huara","Iquique","Pica","Pozo Almonte",
    # Región de Antofagasta
    "Antofagasta","Calama","María Elena","Mejillones","Ollagüe","San Pedro de Atacama","Sierra Gorda","Taltal","Tocopilla",
    # Región de Atacama
    "Alto del Carmen","Caldera","Chañaral","Copiapó","Diego de Almagro","Freirina","Huasco","Tierra Amarilla","Vallenar",
    # Región de Coquimbo
    "Andacollo","Canela","Combarbalá","Coquimbo","Illapel","La Higuera","La Serena","Los Vilos","Monte Patria","Ovalle","Paihuano","Punitaqui","Río Hurtado","Salamanca","Vicuña",
    # Región de Valparaíso
    "Algarrobo","Cabildo","Calle Larga","Cartagena","Casablanca","Catemu","Concón","El Quisco","El Tabo","Hijuelas","Isla de Pascua","Juan Fernández",
    "La Calera","La Cruz","La Ligua","Limache","Llaillay","Los Andes","Nogales","Olmué","Panquehue","Papudo","Petorca","Puchuncaví","Putaendo",
    "Quillota","Quilpué","Quintero","Rinconada","San Antonio","San Esteban","San Felipe","Santa María","Santo Domingo","Valparaíso",
    "Villa Alemana","Viña del Mar","Zapallar",
    # Región Metropolitana de Santiago
    "Alhué","Buin","Calera de Tango","Cerrillos","Cerro Navia","Colina","Conchalí","Curacaví","El Bosque","El Monte","Estación Central","Huechuraba",
    "Independencia","Isla de Maipo","La Cisterna","La Florida","La Granja","Lampa","La Pintana","La Reina","Las Condes","Lo Barnechea","Lo Espejo",
    "Lo Prado","Macul","Maipú","María Pinto","Melipilla","Ñuñoa","Padre Hurtado","Paine","Pedro Aguirre Cerda","Peñaflor","Peñalolén",
    "Pirque","Providencia","Pudahuel","Puente Alto","Quilicura","Quinta Normal","Recoleta","Renca","San Bernardo","San Joaquín",
    "San José de Maipo","San Miguel","San Pedro","San Ramón","Santiago","Talagante","Tiltil","Vitacura",
    # Región del Libertador Gral. Bernardo O'Higgins
    "Chépica","Chimbarongo","Codegua","Coinco","Coltauco","Doñihue","Graneros","La Estrella","Las Cabras","Litueche","Lolol","Machalí","Malloa",
    "Marchihue","Mostazal","Nancagua","Navidad","Olivar","Palmilla","Paredones","Peralillo","Peumo","Pichidegua","Pichilemu","Placilla","Pumanque",
    "Quinta de Tilcoco","Rancagua","Rengo","Requínoa","San Fernando","Santa Cruz","San Vicente",
    # Región del Maule
    "Cauquenes","Chanco","Colbún","Constitución","Curepto","Curicó","Empedrado","Hualañé","Licantén","Linares","Longaví","Maule","Molina",
    "Parral","Pelarco","Pelluhue","Pencahue","Rauco","Retiro","Río Claro","Romeral","Sagrada Familia","San Clemente","San Javier","San Rafael",
    "Talca","Teno","Vichuquén","Villa Alegre","Yerbas Buenas",
    # Región de Ñuble
    "Bulnes","Chillán","Chillán Viejo","Cobquecura","Coelemu","Coihueco","El Carmen","Ninhue","Ñiquén","Pemuco","Pinto","Portezuelo","Quillón",
    "Quirihue","Ránquil","San Carlos","San Fabián","San Ignacio","San Nicolás","Treguaco","Yungay",
    # Región del Biobío
    "Alto Biobío","Antuco","Arauco","Cabrero","Cañete","Chiguayante","Concepción","Contulmo","Coronel","Curanilahue","Florida","Hualpén","Hualqui",
    "Laja","Lebu","Los Alamos","Los Angeles","Lota","Mulchén","Nacimiento","Negrete","Penco","Quilaco","Quilleco","San Pedro de la Paz","San Rosendo",
    "Santa Bárbara","Santa Juana","Talcahuano","Tirúa","Tomé","Tucapel","Yumbel",
    # Región de La Araucanía
    "Angol","Carahue","Cholchol","Collipulli","Cunco","Curacautín","Curarrehue","Ercilla","Freire","Galvarino","Gorbea","Lautaro","Loncoche",
    "Lonquimay","Los Sauces","Lumaco","Melipeuco","Nueva Imperial","Padre Las Casas","Perquenco","Pitrufquén","Pucón","Purén","Renaico","Saavedra",
    "Temuco","Teodoro Schmidt","Toltén","Traiguén","Victoria","Vilcún","Villarrica",
    # Región de Los Ríos
    "Corral","Futrono","Lago Ranco","Lanco","La Unión","Los Lagos","Máfil","Mariquina","Paillaco","Panguipulli","Río Bueno","Valdivia",
    # Región de Los Lagos
    "Ancud","Calbuco","Castro","Chaitén","Chonchi","Cochamó","Curaco de Vélez","Dalcahue","Fresia","Frutillar","Futaleufú","Hualaihué","Llanquihue",
    "Los Muermos","Maullín","Osorno","Palena","Puerto Montt","Puerto Octay","Puerto Varas","Puqueldón","Purranque","Puyehue","Queilén","Quellón",
    "Quemchi","Quinchao","Río Negro","San Juan de la Costa","San Pablo",
    # Región Aysén del G. Carlos Ibáñez del Campo
    "Aysén","Chile Chico","Cisnes","Cochrane","Coyhaique","Guaitecas","Lago Verde","O'Higgins","Río Ibáñez","Tortel",
    # Región de Magallanes y de la Antártica Chilena
    "Antártica","Cabo de Hornos","Laguna Blanca","Natales","Porvenir","Primavera","Punta Arenas","Río Verde","San Gregorio","Timaukel","Torres del Paine",

}

def _title_keep(s: str) -> str:
    return " ".join(w[:1].upper() + w[1:].lower() for w in s.split())

def extract_comuna(location: Optional[str]) -> Optional[str]:
    if not location:
        return None
    loc = re.sub(r"\s*,\s*", ",", location.strip())
    parts = [p for p in loc.split(",") if p]
    for i in range(len(parts) - 1, -1, -1):
        cand = _title_keep(parts[i].strip())
        if cand in COMUNAS:
            return cand
    cand = _title_keep(re.sub(r"\s+", " ", location.strip()))
    if cand in COMUNAS:
        return cand
    return None

