let contenuJSON = [];
let contenuOrdonnancesTypes = [];

// Simuler chrome.storage avec localStorage
const storage = {
    get: function(key, callback) {
        if (typeof key === 'string') {
            const data = localStorage.getItem(key);
            callback({ [key]: data ? JSON.parse(data) : undefined });
        } else {
            const result = {};
            Object.keys(key).forEach(k => {
                const data = localStorage.getItem(k);
                result[k] = data ? JSON.parse(data) : undefined;
            });
            callback(result);
        }
    },
    set: function(data, callback) {
        Object.keys(data).forEach(key => {
            localStorage.setItem(key, JSON.stringify(data[key]));
        });
        if (callback) callback();
    }
};

// === Chargement de la page ===
document.addEventListener("DOMContentLoaded", async () => {
    // === Ajout des écouteurs uniquement quand le DOM est prêt ===
    document.getElementById("choisir-ordonnances-type").addEventListener("click", () => {
        document.getElementById("fichier-ordonnances-type").click();
    });

    document.getElementById("fichier-ordonnances-type").addEventListener("change", handleFileChangeOrdonnancesTypes);

    document.getElementById("ajouter-ordonnances-type").addEventListener("click", ajouterOrdonnancesTypes);

    // Écouteurs pour la gestion des ordonnances types
    document.getElementById("enregistrer-ordonnance").addEventListener("click", enregistrerOrdonnance);
    document.getElementById("annuler-ordonnance").addEventListener("click", annulerOrdonnance);
    document.getElementById("supprimer-ordonnance").addEventListener("click", supprimerOrdonnance);
    document.getElementById("ajouter-medicament").addEventListener("click", ajouterMedicamentForm);
    document.getElementById("recharger-ordonnances-types").addEventListener("click", chargerOrdonnancesTypes);

    // === Exporter médicaments personnalisés ===
    document.getElementById("exporter-medicaments-personnalises").addEventListener("click", function() {
        const medsPersonnalises = JSON.parse(localStorage.getItem('medicamentsPersonnalises') || '[]');

        if (medsPersonnalises.length === 0) {
            showMessage("Aucun médicament personnalisé à exporter.", "red");
            return;
        }

        const dataStr = JSON.stringify(medsPersonnalises, null, 2);
        const dataUri = 'data:application/json;charset=utf-8,' + encodeURIComponent(dataStr);

        const exportFileDefaultName = 'medicaments-personnalises.json';

        const linkElement = document.createElement('a');
        linkElement.setAttribute('href', dataUri);
        linkElement.setAttribute('download', exportFileDefaultName);
        linkElement.click();

        showMessage(`Export réussi : ${medsPersonnalises.length} médicament(s) exporté(s).`, "green");
    });

    // === Importer médicaments personnalisés ===
    const btnImporterMeds = document.getElementById("btn-importer-medicaments");
    const importMedsInput = document.getElementById("importer-medicaments-personnalises");

    if (btnImporterMeds && importMedsInput) {
        btnImporterMeds.addEventListener("click", function() {
            importMedsInput.click();
        });

        importMedsInput.addEventListener("change", function(e) {
            const file = e.target.files[0];
            if (!file) return;

            const reader = new FileReader();
            reader.onload = function(e) {
                try {
                    const importedMeds = JSON.parse(e.target.result);

                    if (!Array.isArray(importedMeds)) {
                        showMessage("Format de fichier invalide. Le fichier doit contenir un tableau de médicaments.", "red");
                        return;
                    }

                    let medsPersonnalises = JSON.parse(localStorage.getItem('medicamentsPersonnalises') || '[]');
                    let nbAjoutes = 0;

                    importedMeds.forEach(med => {
                        if (typeof med === 'string' && med.trim() !== '' && !medsPersonnalises.includes(med.trim())) {
                            medsPersonnalises.push(med.trim());
                            nbAjoutes++;
                        }
                    });

                    localStorage.setItem('medicamentsPersonnalises', JSON.stringify(medsPersonnalises));

                    showMessage(`${nbAjoutes} médicament(s) importé(s) avec succès !`, "green");
                    importMedsInput.value = '';
                } catch (error) {
                    showMessage("Erreur lors de l'import : " + error.message, "red");
                }
            };
            reader.readAsText(file);
        });
    }

    // === Chargement initial ===
    try {
        chargerOrdonnancesTypes();
        showMessage("Données chargées avec succès.", "green");
    } catch (error) {
        showMessage("Erreur lors du chargement des données.", "red");
        console.error("Erreur :", error);
    }
});

// === Fonctions ===

// Gérer le fichier d'ordonnances types
function handleFileChangeOrdonnancesTypes(event) {
    const file = event.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
        try {
            contenuOrdonnancesTypes = JSON.parse(e.target.result);
            if (typeof contenuOrdonnancesTypes !== "object") throw new Error();
            showMessage("Fichier d'ordonnances types chargé avec succès !", "green");
        } catch {
            showMessage("Fichier JSON invalide !", "red");
        }
    };
    reader.readAsText(file);
}

// Ajouter les ordonnances types
function ajouterOrdonnancesTypes() {
    if (Object.keys(contenuOrdonnancesTypes).length === 0) {
        showMessage("Aucun contenu à ajouter.", "red");
        return;
    }

    storage.get("ordonnancesTypes", (result) => {
        let existants = result.ordonnancesTypes || {};
        let fusion = { ...existants, ...contenuOrdonnancesTypes };

        storage.set({ ordonnancesTypes: fusion }, () => {
            localStorage.setItem("ordonnancesTypes", JSON.stringify(fusion));
            // Mettre à jour également la clé utilisée par ord.html
            localStorage.setItem("ordonnancesTypesPourOrd", JSON.stringify(fusion));
            chargerOrdonnancesTypes();
            showMessage("Ordonnances types ajoutées avec succès !", "green");
        });
    });
}

// Charger les ordonnances types
function chargerOrdonnancesTypes() {
    storage.get("ordonnancesTypes", (result) => {
        let data = result.ordonnancesTypes || {};
        remplirListeOrdonnancesTypes(data);
    });
}

// === Charger fichier d'ordonnances pour ord.html ===
const chargerFichierOrdonnancesBtn = document.getElementById("charger-fichier-ordonnances");
const fichierOrdonnancesPourOrdInput = document.getElementById("fichier-ordonnances-pour-ord");

if (chargerFichierOrdonnancesBtn && fichierOrdonnancesPourOrdInput) {
    chargerFichierOrdonnancesBtn.addEventListener("click", function() {
        fichierOrdonnancesPourOrdInput.click();
    });

    fichierOrdonnancesPourOrdInput.addEventListener("change", function(e) {
        const file = e.target.files[0];
        if (!file) return;

        const reader = new FileReader();
        reader.onload = function(e) {
            try {
                const data = JSON.parse(e.target.result);

                if (typeof data !== 'object') {
                    showMessage("Format de fichier invalide. Le fichier doit contenir un objet d'ordonnances types.", "red");
                    return;
                }

                localStorage.setItem('ordonnancesTypesPourOrd', JSON.stringify(data));

                showMessage(`Fichier chargé avec succès ! ${Object.keys(data).length} ordonnance(s) type(s) disponible(s) dans ord.html.`, "green");
                fichierOrdonnancesPourOrdInput.value = '';
            } catch (error) {
                showMessage("Erreur lors du chargement du fichier : " + error.message, "red");
            }
        };
        reader.readAsText(file);
    });
}

// Remplir la liste HTML des ordonnances types
function remplirListeOrdonnancesTypes(data) {
    const container = document.getElementById("ordonnances-liste");
    container.innerHTML = "";

    Object.keys(data).forEach(nom => {
        const ordonnanceItem = document.createElement("div");
        ordonnanceItem.className = "ordonnance-item";
        ordonnanceItem.innerHTML = `
            <strong>${nom}</strong>
            <div class="ordonnance-medicaments">
                ${data[nom].map(med => `
                    <div class="ordonnance-medicament">
                        <span><strong>Médicament:</strong> ${med.medicament}</span>
                        <span><strong>Posologie:</strong> ${med.posologie}</span>
                        <span><strong>Quantité:</strong> ${med.quantite}</span>
                    </div>
                `).join('')}
            </div>
            <div class="ordonnance-actions">
                <button class="modifier-ordonnance btn-secondary" data-nom="${nom}">Modifier</button>
                <button class="supprimer-ordonnance btn-danger" data-nom="${nom}">Supprimer</button>
            </div>
        `;
        container.appendChild(ordonnanceItem);
    });

    // Ajouter les événements pour les boutons de modification et suppression
    document.querySelectorAll('.modifier-ordonnance').forEach(btn => {
        btn.addEventListener('click', function() {
            const nom = this.getAttribute('data-nom');
            modifierOrdonnance(nom);
        });
    });

    document.querySelectorAll('.supprimer-ordonnance').forEach(btn => {
        btn.addEventListener('click', function() {
            const nom = this.getAttribute('data-nom');
            supprimerOrdonnanceDirecte(nom);
        });
    });

    console.log("Liste des ordonnances types mise à jour :", data);
}

// Fonction pour modifier une ordonnance existante
function modifierOrdonnance(nom) {
    storage.get("ordonnancesTypes", (result) => {
        const data = result.ordonnancesTypes || {};
        const ordonnance = data[nom];

        if (ordonnance) {
            // Afficher le formulaire
            document.getElementById("formulaire-ordonnance").classList.remove("hidden");
            document.getElementById("titre-formulaire").textContent = `Modifier l'ordonnance: ${nom}`;
            document.getElementById("nom-ordonnance").value = nom;
            document.getElementById("supprimer-ordonnance").classList.remove("hidden");
            document.getElementById("supprimer-ordonnance").setAttribute("data-nom", nom);

            // Remplir la liste des médicaments
            const listeMeds = document.getElementById("liste-medicaments-ordonnance");
            listeMeds.innerHTML = "";

            ordonnance.forEach((med, index) => {
                const medDiv = document.createElement("div");
                medDiv.className = "form-row";
                medDiv.innerHTML = `
                    <div class="form-group">
                        <label>Médicament</label>
                        <input type="text" class="medicament-input" value="${med.medicament}" placeholder="Nom du médicament">
                    </div>
                    <div class="form-group">
                        <label>Posologie</label>
                        <input type="text" class="posologie-input" value="${med.posologie}" placeholder="Posologie">
                    </div>
                    <div class="form-group">
                        <label>Quantité</label>
                        <input type="text" class="quantite-input" value="${med.quantite}" placeholder="Quantité">
                    </div>
                    <div class="form-group" style="display: flex; align-items: flex-end;">
                        <button type="button" class="btn-danger supprimer-medicament" data-index="${index}">Supprimer</button>
                    </div>
                `;
                listeMeds.appendChild(medDiv);
            });

            // Ajouter les événements pour supprimer des médicaments
            document.querySelectorAll('.supprimer-medicament').forEach(btn => {
                btn.addEventListener('click', function() {
                    const index = parseInt(this.getAttribute('data-index'));
                    const medicamentDiv = this.closest('.form-row');
                    medicamentDiv.remove();
                });
            });

            // Faire défiler vers le formulaire
            document.getElementById("formulaire-ordonnance").scrollIntoView({ behavior: "smooth" });
        }
    });
}

// Fonction pour ajouter un médicament dans le formulaire
function ajouterMedicamentForm() {
    const listeMeds = document.getElementById("liste-medicaments-ordonnance");
    const index = listeMeds.children.length;
    const medDiv = document.createElement("div");
    medDiv.className = "form-row";
    medDiv.innerHTML = `
        <div class="form-group">
            <label>Médicament</label>
            <input type="text" class="medicament-input" placeholder="Nom du médicament">
        </div>
        <div class="form-group">
            <label>Posologie</label>
            <input type="text" class="posologie-input" placeholder="Posologie">
        </div>
        <div class="form-group">
            <label>Quantité</label>
            <input type="text" class="quantite-input" placeholder="Quantité">
        </div>
        <div class="form-group" style="display: flex; align-items: flex-end;">
            <button type="button" class="btn-danger supprimer-medicament" data-index="${index}">Supprimer</button>
        </div>
    `;
    listeMeds.appendChild(medDiv);

    // Ajouter l'événement pour supprimer ce médicament
    const supprimerBtn = medDiv.querySelector('.supprimer-medicament');
    supprimerBtn.addEventListener('click', function() {
        const medicamentDiv = this.closest('.form-row');
        medicamentDiv.remove();
    });
}

// Fonction pour enregistrer une ordonnance (ajouter ou modifier)
function enregistrerOrdonnance() {
    const nom = document.getElementById("nom-ordonnance").value.trim();
    if (!nom) {
        showMessage("Veuillez saisir un nom pour l'ordonnance.", "red");
        return;
    }

    // Récupérer tous les médicaments du formulaire
    const medicamentInputs = document.querySelectorAll('.medicament-input');
    const posologieInputs = document.querySelectorAll('.posologie-input');
    const quantiteInputs = document.querySelectorAll('.quantite-input');

    if (medicamentInputs.length === 0) {
        showMessage("Veuillez ajouter au moins un médicament.", "red");
        return;
    }

    const medicaments = [];
    for (let i = 0; i < medicamentInputs.length; i++) {
        const medicament = medicamentInputs[i].value.trim();
        const posologie = posologieInputs[i].value.trim();
        const quantite = quantiteInputs[i].value.trim();

        if (!medicament || !posologie || !quantite) {
            showMessage(`Veuillez remplir tous les champs pour le médicament ${i + 1}.`, "red");
            return;
        }

        medicaments.push({
            medicament: medicament,
            posologie: posologie,
            quantite: quantite
        });
    }

    // Sauvegarder l'ordonnance
    storage.get("ordonnancesTypes", (result) => {
        const data = result.ordonnancesTypes || {};
        const ancienNom = document.getElementById("nom-ordonnance").value; // Récupérer l'ancien nom si modification

        // Supprimer l'ancienne entrée si c'est une modification
        if (document.getElementById("titre-formulaire").textContent.includes("Modifier")) {
            delete data[ancienNom];
        }

        // Ajouter la nouvelle/la mise à jour
        data[nom] = medicaments;

        storage.set({ ordonnancesTypes: data }, () => {
            localStorage.setItem("ordonnancesTypes", JSON.stringify(data));
            // Mettre à jour également la clé utilisée par ord.html
            localStorage.setItem("ordonnancesTypesPourOrd", JSON.stringify(data));
            chargerOrdonnancesTypes();
            annulerOrdonnance();
            showMessage(`Ordonnance "${nom}" enregistrée avec succès !`, "green");
        });
    });
}

// Fonction pour annuler la modification/ajout d'une ordonnance
function annulerOrdonnance() {
    document.getElementById("formulaire-ordonnance").classList.add("hidden");
    document.getElementById("titre-formulaire").textContent = "Ajouter une nouvelle ordonnance type";
    document.getElementById("nom-ordonnance").value = "";
    document.getElementById("liste-medicaments-ordonnance").innerHTML = "";
    document.getElementById("supprimer-ordonnance").classList.add("hidden");
}

// Fonction pour supprimer une ordonnance (appelée depuis le bouton de suppression)
function supprimerOrdonnance() {
    const nom = document.getElementById("supprimer-ordonnance").getAttribute("data-nom");
    if (confirm(`Êtes-vous sûr de vouloir supprimer l'ordonnance "${nom}" ?`)) {
        supprimerOrdonnanceDirecte(nom);
    }
}

// Fonction pour supprimer une ordonnance directement
function supprimerOrdonnanceDirecte(nom) {
    storage.get("ordonnancesTypes", (result) => {
        const data = result.ordonnancesTypes || {};
        delete data[nom];

        storage.set({ ordonnancesTypes: data }, () => {
            localStorage.setItem("ordonnancesTypes", JSON.stringify(data));
            // Mettre à jour également la clé utilisée par ord.html
            localStorage.setItem("ordonnancesTypesPourOrd", JSON.stringify(data));
            chargerOrdonnancesTypes();
            showMessage(`Ordonnance "${nom}" supprimée avec succès !`, "green");
        });
    });
}

// Afficher un message à l'utilisateur
function showMessage(message, color) {
    const msg = document.getElementById("message");
    msg.textContent = message;
    msg.style.color = color;
}