const storage = chrome.storage.local;

let contenuJSON = [];
let contenuOrdonnancesTypes = [];

// === Chargement de la page ===
document.addEventListener("DOMContentLoaded", async () => {
    // === Ajout des écouteurs uniquement quand le DOM est prêt ===
    document.getElementById("choisir-fichier").addEventListener("click", () => {
        document.getElementById("fichier-json").click();
    });

    document.getElementById("fichier-json").addEventListener("change", handleFileChangeMedicaments);

    document.getElementById("ajouter-contenu").addEventListener("click", ajouterMedicaments);

    document.getElementById("recharger-medicaments").addEventListener("click", async () => {
        try {
            let meds = await chargerMedicaments();
            remplirListeMedicaments(meds);
            showMessage("Médicaments rechargés avec succès.", "green");
        } catch (error) {
            showMessage("Erreur lors du rechargement des médicaments.", "red");
            console.error("Erreur :", error);
        }
    });

    document.getElementById("choisir-ordonnances-type").addEventListener("click", () => {
        document.getElementById("fichier-ordonnances-type").click();
    });

    document.getElementById("fichier-ordonnances-type").addEventListener("change", handleFileChangeOrdonnancesTypes);

    document.getElementById("ajouter-ordonnances-type").addEventListener("click", ajouterOrdonnancesTypes);

    // === Chargement initial ===
    try {
        let meds = await chargerMedicaments();
        remplirListeMedicaments(meds);
        chargerOrdonnancesTypes();
        showMessage("Données chargées avec succès.", "green");
    } catch (error) {
        showMessage("Erreur lors du chargement des données.", "red");
        console.error("Erreur :", error);
    }
});

// === Fonctions ===

// Gérer le fichier de médicaments
function handleFileChangeMedicaments(event) {
    const file = event.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
        try {
            contenuJSON = JSON.parse(e.target.result);
            if (!Array.isArray(contenuJSON)) throw new Error("Le contenu n'est pas un tableau.");
            showMessage("Fichier de médicaments chargé avec succès !", "green");
        } catch {
            showMessage("Fichier JSON invalide !", "red");
        }
    };
    reader.readAsText(file);
}

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

// Ajouter les médicaments au stockage
function ajouterMedicaments() {
    if (contenuJSON.length === 0) {
        showMessage("Aucun contenu à ajouter.", "red");
        return;
    }

    storage.get("medicaments", (result) => {
        let existants = result.medicaments || [];
        let fusion = Array.from(new Set([...existants, ...contenuJSON])).sort((a, b) => a.localeCompare(b));

        storage.set({ medicaments: fusion }, () => {
            localStorage.setItem("medicaments", JSON.stringify(fusion));
            remplirListeMedicaments(fusion);
            showMessage("Médicaments ajoutés avec succès !", "green");
        });
    });
}

// Charger les médicaments
async function chargerMedicaments() {
    return new Promise((resolve) => {
        storage.get("medicaments", (result) => {
            let existants = result.medicaments || [];

            fetch("medicaments.json")
                .then(res => res.json())
                .then(fichier => {
                    let locaux = JSON.parse(localStorage.getItem("medicaments")) || [];
                    let fusion = Array.from(new Set([...existants, ...fichier, ...locaux])).sort((a, b) => a.localeCompare(b));

                    storage.set({ medicaments: fusion }, () => resolve(fusion));
                })
                .catch((err) => {
                    console.error("Erreur JSON local :", err);
                    resolve(existants);
                });
        });
    });
}

// Remplir la liste HTML des médicaments
function remplirListeMedicaments(meds) {
    const liste = document.getElementById("medicaments-liste");
    liste.innerHTML = "";

    meds.forEach(med => {
        const li = document.createElement("li");
        li.textContent = med;
        li.addEventListener("click", () => alert(`Vous avez sélectionné : ${med}`));
        liste.appendChild(li);
    });

    console.log("Liste des médicaments mise à jour :", meds);
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
            remplirListeOrdonnancesTypes(fusion);
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
function remplirListeOrdonnancesTypes(data) {
    const select = document.getElementById("liste-ordonnances-types");
    select.innerHTML = '<option value="">Sélectionnez une ordonnance type</option>'; // Effacer les options existantes

    Object.keys(data).forEach(maladie => {
        const option = document.createElement("option");
        option.value = maladie;
        option.textContent = maladie;
        select.appendChild(option);
    });

    console.log("Ordonnances types chargées :", Object.keys(data));
}
// Remplir la liste HTML des ordonnances types
function remplirListeOrdonnancesTypes2(data) {
    const liste = document.getElementById("ordonnances-liste");
    liste.innerHTML = "";

    Object.keys(data).forEach(nom => {
        const li = document.createElement("li");
        li.textContent = nom;
        li.addEventListener("click", () => alert(`Vous avez sélectionné : ${nom}`));
        liste.appendChild(li);
    });

    console.log("Liste des ordonnances types mise à jour :", data);
}

function ajouterOrdonnancesTypes() {
    if (Object.keys(contenuOrdonnancesTypes).length === 0) {
        showMessage("Aucun contenu à ajouter.", "red");
        return;
    }

    storage.get("ordonnancesTypes", (result) => {
        let existants = result.ordonnancesTypes || {};  // Obtenir les ordonnances existantes
        let fusion = { ...existants, ...contenuOrdonnancesTypes };  // Fusionner les objets

        storage.set({ ordonnancesTypes: fusion }, () => {
            localStorage.setItem("ordonnancesTypes", JSON.stringify(fusion));
            remplirListeOrdonnancesTypes(fusion);  // Mettre à jour la liste
            showMessage("Ordonnances types ajoutées avec succès !", "green");
        });
    });
}


// Afficher un message à l'utilisateur
function showMessage(message, color) {
    const msg = document.getElementById("message");
    msg.textContent = message;
    msg.style.color = color;
}