// popup.js
// Écouter les messages pour ouvrir popup.js
chrome.runtime.onMessage.addListener((message) => {
    if (message.action === "ouvrirPopup") {
        // Ouvrir la modale ou effectuer d'autres actions necessaires
        document.getElementById("modal").style.display = "block";
        // Vous pouvez egalement appeler d'autres fonctions ici si necessaire
    }
});


// Declaration de la variable pour stocker les medicaments
const ordonnanceMedicaments = [];
// Gerer l'ouverture de la modale
document.getElementById("gerer-medicaments").addEventListener("click", () => {
    afficherMedicamentsPersonnalises();
    document.getElementById("modal").style.display = "block";
});



// Gerer la fermeture de la modale
document.querySelector(".close").onclick = function () {
    document.getElementById("modal").style.display = "none";
};

window.onclick = function (event) {
    if (event.target == document.getElementById("modal")) {
        document.getElementById("modal").style.display = "none";
    }
};

// Vider les medicaments
document.getElementById("vider-medicaments").addEventListener("click", () => {
    if (confirm("Voulez-vous vraiment supprimer tous les medicaments enregistres ?")) {
        localStorage.setItem("medicaments", JSON.stringify([]));
        afficherMedicamentsPersonnalises();
        remplirDatalist([]);
    }
});

// Fonction pour capitaliser automatiquement les noms et prénoms
function capitalizeNames(text) {
    if (!text) return text;
    return text.toLowerCase().replace(/\b\w/g, letter => letter.toUpperCase());
}

document.addEventListener("DOMContentLoaded", async () => {
    // Gestion de la navigation au clavier entre les champs
    const champMedicament = document.getElementById("medicament");
    const champDosage = document.getElementById("dosage");
    const champDuree = document.getElementById("duree");
    const champFrequence = document.getElementById("frequence");
    const champQuantite = document.getElementById("quantite");
    const champPosologie = document.getElementById("posologie");

    champMedicament.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
            e.preventDefault();
            champDosage.focus();
        }
    });

    champDosage.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
            e.preventDefault();
            champDuree.focus();
        }
    });

    champDuree.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
            e.preventDefault();
            champFrequence.focus();
        }
    });

    champFrequence.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
            e.preventDefault();
            champQuantite.focus();
        }
    });

    champQuantite.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
            e.preventDefault();
            champPosologie.focus();
        }
    });

    // Charger les données de stockage
    chrome.storage.local.get(['nom', 'prenom', 'dob', 'poids'], (data) => {
        document.getElementById("nom").value = data.nom || "";
        document.getElementById("prenom").value = data.prenom || "";
        document.getElementById("dob").value = data.dob || "";
        document.getElementById("poids").value = data.poids || "";
    });

    // Charger les médicaments
    try {
        let meds = await chargerMedicaments();
        remplirDatalist(meds);
    } catch (error) {
        console.error("Erreur lors du chargement des médicaments:", error);
    }
});

// Charger les médicaments depuis le stockage ou le fichier local
async function chargerMedicaments() {
    return new Promise((resolve) => {
        chrome.storage.local.get("medicaments", (result) => {
            let meds = result.medicaments || [];
            resolve(meds);
        });
    });
}

// Remplir le datalist avec les médicaments
function remplirDatalist(medicaments) {
    const datalist = document.getElementById("medicaments-list");
    datalist.innerHTML = "";

    medicaments.forEach(med => {
        const option = document.createElement("option");
        option.value = med;
        datalist.appendChild(option);
    });
}

// Ajouter un médicament à l'ordonnance
document.getElementById("ajouter-medicament").addEventListener("click", () => {
    const medicament = document.getElementById("medicament").value.trim();
    const dosage = document.getElementById("dosage").value.trim();
    const duree = document.getElementById("duree").value.trim();
    const frequence = document.getElementById("frequence").value.trim();
    const quantite = document.getElementById("quantite").value.trim();
    const posologie = document.getElementById("posologie").value.trim();

    if (!medicament) {
        alert("Veuillez entrer un médicament");
        return;
    }

    const medicamentObj = {
        medicament,
        dosage,
        duree,
        frequence,
        quantite,
        posologie
    };

    ordonnanceMedicaments.push(medicamentObj);
    afficherMedicamentsPersonnalises();

    // Réinitialiser les champs
    document.getElementById("medicament").value = "";
    document.getElementById("dosage").value = "";
    document.getElementById("duree").value = "";
    document.getElementById("frequence").value = "";
    document.getElementById("quantite").value = "";
    document.getElementById("posologie").value = "";

    // Mettre le focus sur le premier champ
    document.getElementById("medicament").focus();
});

// Afficher les médicaments ajoutés
function afficherMedicamentsPersonnalises() {
    const liste = document.getElementById("medicaments-personnalises");
    liste.innerHTML = "";

    if (ordonnanceMedicaments.length === 0) {
        liste.innerHTML = "<li>Aucun médicament ajouté</li>";
        return;
    }

    ordonnanceMedicaments.forEach((med, index) => {
        const li = document.createElement("li");
        li.textContent = `${med.medicament} - ${med.dosage} - ${med.duree} - ${med.frequence} - ${med.quantite} - ${med.posologie}`;
        li.innerHTML += `<button class="supprimer" data-index="${index}">Supprimer</button>`;
        liste.appendChild(li);
    });

    // Ajouter les écouteurs pour les boutons de suppression
    document.querySelectorAll(".supprimer").forEach(btn => {
        btn.addEventListener("click", (e) => {
            const index = parseInt(e.target.getAttribute("data-index"));
            ordonnanceMedicaments.splice(index, 1);
            afficherMedicamentsPersonnalises();
        });
    });
}

// Générer l'ordonnance
document.getElementById("generer-ordonnance").addEventListener("click", async () => {
    const nom = document.getElementById("nom").value.trim();
    const prenom = document.getElementById("prenom").value.trim();
    const dob = document.getElementById("dob").value.trim();
    const poids = document.getElementById("poids").value.trim();

    if (!nom || !prenom || !dob || !poids) {
        alert("Veuillez remplir tous les champs du patient");
        return;
    }

    if (ordonnanceMedicaments.length === 0) {
        alert("Veuillez ajouter au moins un médicament");
        return;
    }

    // Stocker les données du patient
    chrome.storage.local.set({
        nom,
        prenom,
        dob,
        poids
    });

    // Stocker les médicaments
    chrome.storage.local.set({
        medicaments: ordonnanceMedicaments
    });

    // Ouvrir la page d'ordonnance
    chrome.tabs.create({ url: chrome.runtime.getURL('ord.html') });
});