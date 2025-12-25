class VaccinationApp {
    constructor() {
        this.data = {
            dateNaissance: '',
            nomEnfant: '',
            prenomEnfant: '',
            vaccinations: {},
            conditions: {
                poidsInf2000: false,
                mereHbsPositif: false
            }
        };

        this.vaccineConfig = {
            "BCG": {
                icon: "🛡️",
                category: "Naissance",
                message: "Vaccin contre la tuberculose. Première dose recommandée dès la naissance.",
                minAgeDays: 0,
                dependsOn: null,
                minIntervalDays: 0,
                color: "#1976D2"
            },
            "Hépatite B 1": {
                icon: "💉",
                category: "Naissance",
                message: "Hépatite B 1 - L'enfant doit avoir moins de 7 jours, ou bien il a un poids inférieur à 2000g, ou sa mère est HBs positive et son âge inférieur à 30 jours.",
                minAgeDays: 0,
                dependsOn: null,
                minIntervalDays: 0,
                color: "#4CAF50"
            },
            "DTCa-VPI-Hib-HBV 1": {
                icon: "🔬",
                category: "Primo-vaccination",
                message: "DTCa-VPI-Hib-HBV 1 - Âge minimum 2 mois. Primo-vaccination hexavalente.",
                minAgeDays: 60,
                dependsOn: null,
                minIntervalDays: 0,
                color: "#FF9800"
            },
            "DTCa-VPI-Hib-HBV 2": {
                icon: "🔬",
                category: "Primo-vaccination",
                message: "DTCa-VPI-Hib-HBV 2 - Âge minimum 4 mois. Intervalle minimum 6 semaines après la 1ère dose.",
                minAgeDays: 120,
                dependsOn: "DTCa-VPI-Hib-HBV 1",
                minIntervalDays: 42,
                color: "#FF9800"
            },
            "DTCa-VPI-Hib-HBV Rappel": {
                icon: "🔬",
                category: "Rappel",
                message: "DTCa-VPI-Hib-HBV Rappel - Rappel à partir de 12 mois. Intervalle minimum 6 mois après la 2ème dose (minimum 4 mois).",
                minAgeDays: 365,
                dependsOn: "DTCa-VPI-Hib-HBV 2",
                minIntervalDays: 180,
                color: "#FF9800"
            },
            "VPC 13 - 1": {
                icon: "🫁",
                category: "Pneumocoque",
                message: "VPC 13 - 1 - Âge minimum 2 mois. Rattrapage possible jusqu'à 2 ans.",
                minAgeDays: 60,
                dependsOn: null,
                minIntervalDays: 0,
                color: "#9C27B0"
            },
            "VPC 13 - 2": {
                icon: "🫁",
                category: "Pneumocoque",
                message: "VPC 13 - 2 - Âge minimum 4 mois. Intervalle minimum 6 semaines après la 1ère dose.",
                minAgeDays: 120,
                dependsOn: "VPC 13 - 1",
                minIntervalDays: 42,
                color: "#9C27B0"
            },
            "VPC 13 - Rappel": {
                icon: "🫁",
                category: "Pneumocoque",
                message: "VPC 13 - Rappel - Rappel à partir de 12 mois. Intervalle minimum 6 mois après la 2ème dose.",
                minAgeDays: 365,
                dependsOn: "VPC 13 - 2",
                minIntervalDays: 180,
                color: "#9C27B0"
            },
            "ROR 1": {
                icon: "🦠",
                category: "Rougeole",
                message: "ROR 1 - Âge minimum 11 mois (1ère dose).",
                minAgeDays: 330,
                dependsOn: null,
                minIntervalDays: 0,
                color: "#FECA57"
            },
            "ROR 2": {
                icon: "🦠",
                category: "Rougeole",
                message: "ROR 2 - Âge minimum 18 mois. Intervalle minimum 1 mois après ROR 1. La 2ème dose n'est pas un rappel.",
                minAgeDays: 540,
                dependsOn: "ROR 1",
                minIntervalDays: 30,
                color: "#FECA57"
            }
        };

        this.init();
    }

    init() {
        try {
            console.log("🚀 Initialisation de l'application...");
            this.setupEventListeners();
            this.setupTabs();
            this.loadStoredData();
            this.forceUpdateAllStatuses();
            this.showMessage("🎯 Application Web - Interface optimisée chargée\n\n📝 Saisissez les informations de l'enfant pour commencer");
            console.log("✅ Application initialisée avec succès");
        } catch (error) {
            console.error("❌ Erreur lors de l'initialisation:", error);
            this.showMessage("❌ Erreur lors de l'initialisation de l'application");
        }
    }

    // Nouvelle fonction pour forcer la mise à jour de tous les statuts
    forceUpdateAllStatuses() {
        // Attendre que le DOM soit complètement chargé
        setTimeout(() => {
            this.collectData();
            this.updateAllStatuses();
        }, 100);
    }

    setupTabs() {
        try {
            const tabButtons = document.querySelectorAll('.tab-button');
            const tabContents = document.querySelectorAll('.tab-content');

            console.log(`🔍 Trouvé ${tabButtons.length} boutons d'onglets`);
            console.log(`🔍 Trouvé ${tabContents.length} contenus d'onglets`);

            if (tabButtons.length === 0) {
                console.error("❌ Aucun bouton d'onglet trouvé!");
                return;
            }

            tabButtons.forEach((button, index) => {
                console.log(`🔍 Configuration onglet ${index}:`, button.getAttribute('data-tab'));

                button.addEventListener('click', (e) => {
                    console.log(`🖱️ Clic sur onglet:`, button.getAttribute('data-tab'));

                    const targetTab = button.getAttribute('data-tab');

                    // Désactiver tous les onglets
                    tabButtons.forEach(btn => btn.classList.remove('active'));
                    tabContents.forEach(content => content.classList.remove('active'));

                    // Activer l'onglet sélectionné
                    button.classList.add('active');
                    const targetContent = document.getElementById(`${targetTab}-tab`);

                    if (targetContent) {
                        targetContent.classList.add('active');
                        console.log(`✅ Onglet ${targetTab} activé`);
                    } else {
                        console.error(`❌ Contenu d'onglet ${targetTab}-tab non trouvé!`);
                    }

                    // Mettre à jour les statuts selon l'onglet
                    try {
                        if (targetTab === 'actions') {
                            this.updateVaccinationStatus();
                        } else if (targetTab === 'vaccinations') {
                            this.updateQuickSummary();
                        }
                    } catch (error) {
                        console.error(`❌ Erreur lors de la mise à jour des statuts:`, error);
                    }
                });
            });

            console.log("✅ Onglets configurés avec succès");

        } catch (error) {
            console.error("❌ Erreur lors de la configuration des onglets:", error);
        }
    }

    setupEventListeners() {
        // Informations enfant
        document.getElementById('birthDate').addEventListener('change', () => this.onDateChange());
        document.getElementById('patientName').addEventListener('input', () => this.collectData());

        // Conditions spéciales
        document.getElementById('lowWeight').addEventListener('change', () => this.onConditionChange());
        document.getElementById('hbPositiveMother').addEventListener('change', () => this.onConditionChange());

        // Champs de vaccination
        document.querySelectorAll('.vaccine-date').forEach(input => {
            input.addEventListener('change', (e) => this.onVaccineChange(e.target));
            input.addEventListener('focus', (e) => this.onVaccineFocus(e.target));
        });

        // Boutons d'action
        document.getElementById('btnReset').addEventListener('click', () => this.resetForm());
        document.getElementById('btnSave').addEventListener('click', () => this.saveData());
        document.getElementById('btnLoad').addEventListener('click', () => this.loadData());
        document.getElementById('btnReport').addEventListener('click', () => this.generateReport());
        document.getElementById('btnSchema').addEventListener('click', () => this.proposeVaccinationSchema());

        // Modales
        document.querySelectorAll('.close').forEach(closeBtn => {
            closeBtn.addEventListener('click', (e) => this.closeModal(e.target.closest('.modal')));
        });

        // Fermer modal en cliquant à l'extérieur
        window.addEventListener('click', (e) => {
            if (e.target.classList.contains('modal')) {
                this.closeModal(e.target);
            }
        });

        // Export et impression
        document.getElementById('btnExportSchema')?.addEventListener('click', () => this.exportSchema());
        document.getElementById('btnPrintSchema')?.addEventListener('click', () => this.printSchema());
    }

    onDateChange() {
        const dateInput = document.getElementById('birthDate');
        const ageDisplay = document.getElementById('patientAgeDisplay');

        if (dateInput.value) {
            const birthDate = new Date(dateInput.value);
            const currentDate = new Date();
            const ageDays = Math.floor((currentDate - birthDate) / (1000 * 60 * 60 * 24));

            if (ageDays >= 0) {
                const years = Math.floor(ageDays / 365);
                const months = Math.floor((ageDays % 365) / 30);
                const days = ageDays % 30;

                ageDisplay.textContent = `🎂 ${years} ans, ${months} mois, ${days} jours`;
                ageDisplay.style.color = '#27ae60';
                ageDisplay.style.display = 'block';

                this.data.dateNaissance = dateInput.value;
                this.revalidateAllVaccines();
            } else {
                ageDisplay.textContent = '⚠️ Date future';
                ageDisplay.style.color = '#f39c12';
                ageDisplay.style.display = 'block';
            }
        } else {
            ageDisplay.style.display = 'none';
        }

        this.collectData();
        this.updateQuickSummary();
        this.updateVaccinationStatus();
    }

    onConditionChange() {
        this.data.conditions.poidsInf2000 = document.getElementById('lowWeight').checked;
        this.data.conditions.mereHbsPositif = document.getElementById('hbPositiveMother').checked;
        this.revalidateAllVaccines();
        this.collectData();
        this.updateQuickSummary();
        this.updateVaccinationStatus();
    }

    onVaccineChange(input) {
        const vaccineName = this.getVaccineNameFromInput(input);
        this.validateVaccine(vaccineName, input);
        this.collectData();

        // Forcer la mise à jour immédiate des statuts
        setTimeout(() => {
            this.updateAllStatuses();
        }, 50);
    }

    onVaccineFocus(input) {
        const vaccineName = this.getVaccineNameFromInput(input);
        const config = this.vaccineConfig[vaccineName];
        if (config) {
            this.showMessage(`💡 ${vaccineName}\n\n${config.message}`);
        }
    }

    getVaccineNameFromInput(input) {
        const mapping = {
            'vaccine-BCG': 'BCG',
            'vaccine-HepatiteB1': 'Hépatite B 1',
            'vaccine-DTCa1': 'DTCa-VPI-Hib-HBV 1',
            'vaccine-DTCa2': 'DTCa-VPI-Hib-HBV 2',
            'vaccine-DTCaRappel': 'DTCa-VPI-Hib-HBV Rappel',
            'vaccine-VPC1': 'VPC 13 - 1',
            'vaccine-VPC2': 'VPC 13 - 2',
            'vaccine-VPCRappel': 'VPC 13 - Rappel',
            'vaccine-ROR1': 'ROR 1',
            'vaccine-ROR2': 'ROR 2'
        };
        
        // Parcourir tous les mappings pour trouver celui qui correspond
        for (const [key, value] of Object.entries(mapping)) {
            if (input.classList.contains('vaccine-date') && input.getAttribute('data-vaccine') === value) {
                return value;
            }
        }
        return input.getAttribute('data-vaccine') || '';
    }

    validateVaccine(vaccineName, input) {
        const vaccineField = input.closest('.vaccine-field');

        if (!input.value) {
            this.resetVaccineValidation(vaccineField, null);
            return;
        }

        const isValid = this.validateVaccineLogic(vaccineName, input.value);

        if (isValid) {
            vaccineField.classList.remove('error');
            vaccineField.classList.add('valid');
            input.style.borderColor = '#27ae60';
            input.style.backgroundColor = '#d5f4e6';
        } else {
            vaccineField.classList.remove('valid');
            vaccineField.classList.add('error');
            input.style.borderColor = '#e74c3c';
            input.style.backgroundColor = '#fadbd8';
        }
    }

    validateVaccineLogic(vaccineName, vaccineDate) {
        try {
            const vaccineDateObj = new Date(vaccineDate);
            const birthDate = this.data.dateNaissance;

            if (!birthDate) return false;

            const birthDateObj = new Date(birthDate);
            const ageDays = Math.floor((vaccineDateObj - birthDateObj) / (1000 * 60 * 60 * 24));

            const config = this.vaccineConfig[vaccineName];
            if (!config) return false;

            // Validation spéciale pour BCG
            if (vaccineName === "BCG") {
                return this.checkBCGConditions(ageDays);
            }

            // Validation spéciale pour Hépatite B 1
            if (vaccineName === "Hépatite B 1") {
                return this.checkHepatitisConditions(ageDays);
            }

            // Validation normale
            if (ageDays < config.minAgeDays) return false;

            // Vérifier âge maximum
            if (config.maxAgeDays && ageDays > config.maxAgeDays) return false;

            // Vérifier dépendances
            if (config.dependsOn) {
                const prevVaccineInput = this.getInputByVaccineName(config.dependsOn);
                if (!prevVaccineInput || !prevVaccineInput.value) return false;

                const prevDateObj = new Date(prevVaccineInput.value);
                const intervalDays = Math.floor((vaccineDateObj - prevDateObj) / (1000 * 60 * 60 * 24));

                if (intervalDays < config.minIntervalDays) return false;
            }

            return true;
        } catch (error) {
            return false;
        }
    }

    checkBCGConditions(ageDays) {
        // BCG : Recommandé avant la sortie de maternité (généralement dans les premiers jours)
        // Peut être fait plus tard si nécessaire, pas de limite stricte d'âge
        return true; // Le BCG peut être fait à tout âge si indiqué
    }

    checkHepatitisConditions(ageDays) {
        if (ageDays <= 7) return true;
        if (this.data.conditions.poidsInf2000) return true;
        if (this.data.conditions.mereHbsPositif && ageDays <= 30) return true;
        return false;
    }

    getInputByVaccineName(vaccineName) {
        const mapping = {
            'BCG': 'vaccine-BCG',
            'Hépatite B 1': 'vaccine-HepatiteB1',
            'DTCa-VPI-Hib-HBV 1': 'vaccine-DTCa1',
            'DTCa-VPI-Hib-HBV 2': 'vaccine-DTCa2',
            'DTCa-VPI-Hib-HBV Rappel': 'vaccine-DTCaRappel',
            'VPC 13 - 1': 'vaccine-VPC1',
            'VPC 13 - 2': 'vaccine-VPC2',
            'VPC 13 - Rappel': 'vaccine-VPCRappel',
            'ROR 1': 'vaccine-ROR1',
            'ROR 2': 'vaccine-ROR2'
        };
        const inputId = mapping[vaccineName];
        return inputId ? document.querySelector(`.vaccine-date[data-vaccine="${vaccineName}"]`) : null;
    }

    resetVaccineValidation(vaccineField, statusIcon) {
        vaccineField.classList.remove('valid', 'error');
        const input = vaccineField.querySelector('.vaccine-date');
        if (input) {
            input.style.borderColor = '';
            input.style.backgroundColor = '';
        }
    }

    revalidateAllVaccines() {
        document.querySelectorAll('.vaccine-date').forEach(input => {
            if (input.value) {
                const vaccineName = this.getVaccineNameFromInput(input);
                this.validateVaccine(vaccineName, input);
            }
        });
    }

    collectData() {
        const fullName = document.getElementById('patientName').value.split(' ');
        this.data.prenomEnfant = fullName[0] || '';
        this.data.nomEnfant = fullName.slice(1).join(' ') || '';
        this.data.dateNaissance = document.getElementById('birthDate').value;

        // Réinitialiser les vaccinations
        this.data.vaccinations = {};

        // Collecter toutes les vaccinations (même vides pour avoir une vue complète)
        document.querySelectorAll('.vaccine-date').forEach(input => {
            const vaccineName = input.getAttribute('data-vaccine');
            if (vaccineName) {
                if (input.value) {
                    this.data.vaccinations[vaccineName] = input.value;
                }
                // Note: on ne stocke que les dates remplies, mais on vérifie tous les champs
            }
        });

        this.saveToStorage();
    }

    resetForm() {
        // Effet visuel sur le bouton Reset
        const resetBtn = document.getElementById('btnReset');
        this.addResetButtonEffect(resetBtn);

        // Effet de fade out sur les champs avant réinitialisation
        const fieldsToReset = [
            document.getElementById('birthDate'),
            document.getElementById('patientName'),
            document.getElementById('patientAgeDisplay'),
            ...document.querySelectorAll('.vaccine-date')
        ];

        // Animation de fade out
        fieldsToReset.forEach(field => {
            if (field) {
                field.style.transition = 'opacity 0.3s ease-out';
                field.style.opacity = '0.3';
            }
        });

        // Attendre la fin de l'animation avant de réinitialiser
        setTimeout(() => {
            // Réinitialiser les champs
            document.getElementById('birthDate').value = '';
            document.getElementById('patientName').value = '';
            document.getElementById('lowWeight').checked = false;
            document.getElementById('hbPositiveMother').checked = false;
            document.getElementById('patientAgeDisplay').style.display = 'none';

            // Réinitialiser les vaccins
            document.querySelectorAll('.vaccine-date').forEach(input => {
                input.value = '';
                const vaccineField = input.closest('.vaccine-field');
                this.resetVaccineValidation(vaccineField, null);
            });

            // Réinitialiser les données
            this.data = {
                dateNaissance: '',
                nomEnfant: '',
                prenomEnfant: '',
                vaccinations: {},
                conditions: {
                    poidsInf2000: false,
                    mereHbsPositif: false
                }
            };

            // Animation de fade in après réinitialisation
            fieldsToReset.forEach(field => {
                if (field) {
                    field.style.opacity = '1';
                }
            });

            // Mettre à jour les statuts après réinitialisation
            this.updateQuickSummary();
            this.updateVaccinationStatus();

            this.showMessage("🔄 Formulaire réinitialisé avec succès");
            this.saveToStorage();
        }, 300);
    }

    addResetButtonEffect(button) {
        // Sauvegarder le style original
        const originalBg = button.style.backgroundColor;

        // Ajouter la transition pour un effet fluide
        button.style.transition = 'background-color 0.3s ease';

        // Changement de couleur simple
        button.style.backgroundColor = '#ff6b6b';

        // Retour à l'état normal après 500ms
        setTimeout(() => {
            button.style.backgroundColor = originalBg;
        }, 500);
    }

    saveData() {
        this.collectData();
        const dataStr = JSON.stringify(this.data, null, 2);
        const blob = new Blob([dataStr], { type: 'application/json' });
        const url = URL.createObjectURL(blob);

        const a = document.createElement('a');
        a.href = url;
        a.download = `vaccination_${this.data.nomEnfant || 'enfant'}_${new Date().toISOString().split('T')[0]}.json`;
        a.click();

        URL.revokeObjectURL(url);
        this.showMessage("💾 Données sauvegardées avec succès");
    }

    loadData() {
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = '.json';
        input.onchange = (e) => {
            const file = e.target.files[0];
            if (file) {
                const reader = new FileReader();
                reader.onload = (e) => {
                    try {
                        const data = JSON.parse(e.target.result);
                        this.loadDataFromObject(data);
                        this.showMessage(`📂 Données chargées: ${file.name}`);
                    } catch (error) {
                        this.showMessage("❌ Erreur lors du chargement du fichier");
                    }
                };
                reader.readAsText(file);
            }
        };
        input.click();
    }

    loadDataFromObject(data) {
        document.getElementById('birthDate').value = data.dateNaissance || '';
        document.getElementById('patientName').value = (data.prenomEnfant || '') + (data.nomEnfant ? ' ' + data.nomEnfant : '');
        document.getElementById('lowWeight').checked = data.conditions?.poidsInf2000 || false;
        document.getElementById('hbPositiveMother').checked = data.conditions?.mereHbsPositif || false;

        // Charger les vaccinations
        Object.entries(data.vaccinations || {}).forEach(([vaccineName, date]) => {
            const input = this.getInputByVaccineName(vaccineName);
            if (input) {
                input.value = date;
            }
        });

        this.onDateChange();
        this.onConditionChange();
        this.revalidateAllVaccines();

        // Forcer la mise à jour des statuts après chargement
        setTimeout(() => {
            this.updateQuickSummary();
            this.updateVaccinationStatus();
        }, 200);
    }

    loadStoredData() {
        try {
            const storedData = localStorage.getItem('vaccinationData');
            if (storedData) {
                const data = JSON.parse(storedData);
                this.loadDataFromObject(data);
            }
        } catch (error) {
            console.error("Erreur lors du chargement des données stockées:", error);
        }
    }

    saveToStorage() {
        try {
            localStorage.setItem('vaccinationData', JSON.stringify(this.data));
        } catch (error) {
            console.error("Erreur lors de la sauvegarde des données:", error);
        }
    }

    generateReport() {
        this.collectData();

        if (!this.data.nomEnfant && !this.data.prenomEnfant) {
            this.showMessage("⚠️ Nom de l'enfant requis pour générer le rapport");
            return;
        }

        if (!this.data.dateNaissance) {
            this.showMessage("⚠️ Date de naissance requise pour générer le rapport");
            return;
        }

        this.showDetailedReport();
    }

    showDetailedReport() {
        const modal = document.getElementById('reportModal');
        const content = document.getElementById('reportContent');

        const birthDate = new Date(this.data.dateNaissance);
        const currentDate = new Date();
        const ageDays = Math.floor((currentDate - birthDate) / (1000 * 60 * 60 * 24));
        const years = Math.floor(ageDays / 365);
        const months = Math.floor((ageDays % 365) / 30);

        // Créer le contenu du rapport
        let reportHTML = `
            <div class="report-header">
                <h3>👤 Informations de l'enfant</h3>
                <p><strong>Nom:</strong> ${this.data.prenomEnfant} ${this.data.nomEnfant}</p>
                <p><strong>Date de naissance:</strong> ${new Date(this.data.dateNaissance).toLocaleDateString('fr-FR')}</p>
                <p><strong>Âge:</strong> ${years} ans, ${months} mois</p>
            </div>

            <div class="report-conditions">
                <h3>⚠️ Conditions Spéciales</h3>
                <p>Poids < 2000g: ${this.data.conditions.poidsInf2000 ? '✅ Oui' : '❌ Non'}</p>
                <p>Mère HBs positif: ${this.data.conditions.mereHbsPositif ? '✅ Oui' : '❌ Non'}</p>
            </div>

            <div class="report-vaccinations">
                <h3>💉 État Vaccinal</h3>
                <table style="width: 100%; border-collapse: collapse;">
                    <thead>
                        <tr style="background: #f8f9fa;">
                            <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Vaccin</th>
                            <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Date</th>
                            <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Statut</th>
                        </tr>
                    </thead>
                    <tbody>
        `;

        Object.keys(this.vaccineConfig).forEach(vaccineName => {
            const date = this.data.vaccinations[vaccineName] || '';
            const isValid = date ? this.validateVaccineLogic(vaccineName, date) : false;
            const status = date ? (isValid ? '✅ Valide' : '❌ Invalide') : '⏳ Non fait';

            reportHTML += `
                <tr>
                    <td style="border: 1px solid #ddd; padding: 8px;">${this.vaccineConfig[vaccineName].icon} ${vaccineName}</td>
                    <td style="border: 1px solid #ddd; padding: 8px;">${date ? new Date(date).toLocaleDateString('fr-FR') : '-'}</td>
                    <td style="border: 1px solid #ddd; padding: 8px;">${status}</td>
                </tr>
            `;
        });

        reportHTML += `
                    </tbody>
                </table>
            </div>

            <div class="report-footer" style="margin-top: 20px; font-size: 0.9em; color: #666;">
                <p><small>Rapport généré le ${new Date().toLocaleDateString('fr-FR')} à ${new Date().toLocaleTimeString('fr-FR')}</small></p>
                <p><small>Par Application Web - Schéma de Rattrapage Vaccinal</small></p>
            </div>
        `;

        content.innerHTML = reportHTML;
        modal.style.display = 'block';
    }

    proposeVaccinationSchema() {
        this.collectData();

        if (!this.data.dateNaissance) {
            this.showMessage("⚠️ Date de naissance requise pour proposer un schéma");
            return;
        }

        if (!this.data.nomEnfant && !this.data.prenomEnfant) {
            this.showMessage("⚠️ Nom de l'enfant requis pour proposer un schéma");
            return;
        }

        this.showVaccinationSchema();
    }

    showVaccinationSchema() {
        const modal = document.getElementById('schemaModal');
        const content = document.getElementById('schemaContent');

        const birthDate = new Date(this.data.dateNaissance);
        const currentDate = new Date();
        const ageDays = Math.floor((currentDate - birthDate) / (1000 * 60 * 60 * 24));
        const years = Math.floor(ageDays / 365);
        const months = Math.floor((ageDays % 365) / 30);

        // Créer le schéma optimisé pour l'impression
        let schemaHTML = `
            <div class="schema-print">
                <div class="schema-header">
                    <h1>SCHÉMA DE RATTRAPAGE VACCINAL</h1>
                    <div class="patient-info">
                        <div><strong>Enfant:</strong> ${this.data.prenomEnfant} ${this.data.nomEnfant}</div>
                        <div><strong>Né(e) le:</strong> ${birthDate.toLocaleDateString('fr-FR')} (${years} ans, ${months} mois)</div>
                        <div><strong>Généré le:</strong> ${currentDate.toLocaleDateString('fr-FR')}</div>
                        ${this.data.conditions.poidsInf2000 ? '<div><strong>⚠️ Condition:</strong> Poids < 2000g</div>' : ''}
                        ${this.data.conditions.mereHbsPositif ? '<div><strong>⚠️ Condition:</strong> Mère HBs positif</div>' : ''}
                    </div>
                </div>

                <h2>📋 Schéma de Vaccination Détaillé</h2>
        `;

        // Organiser les vaccins par catégorie
        const categories = {
            'Naissance': [],
            'Primo-vaccination': [],
            'Rappel': [],
            'Pneumocoque': [],
            'Rougeole': []
        };

        Object.entries(this.vaccineConfig).forEach(([vaccineName, config]) => {
            categories[config.category].push([vaccineName, config]);
        });

        // Générer les lignes du tableau avec calcul séquentiel des dates
        const proposedDates = {}; // Pour stocker les dates proposées

        Object.entries(categories).forEach(([category, vaccines]) => {
            // Ligne de catégorie
            schemaHTML += `<h3>${category}</h3>`;
            
            // Tableau pour cette catégorie
            schemaHTML += `
                <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
                    <thead>
                        <tr style="background-color: #f0f0f0;">
                            <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Vaccin</th>
                            <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Date Actuelle</th>
                            <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Âge Actuel</th>
                            <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Statut</th>
                            <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Date Recommandée</th>
                            <th style="border: 1px solid #ddd; padding: 6px; text-align: left;">Observations</th>
                        </tr>
                    </thead>
                    <tbody>
            `;

            // Lignes des vaccins
            vaccines.forEach(([vaccineName, config]) => {
                // Nom du vaccin
                const nameCell = `${config.icon} ${vaccineName}`;

                // Date actuelle
                const currentVaccination = this.data.vaccinations[vaccineName];
                const currentDateCell = currentVaccination || '-';

                // Âge actuel lors de la vaccination
                let currentAgeText = '-';
                if (currentVaccination) {
                    const vaccineDate = new Date(currentVaccination);
                    const ageDaysAtVaccine = Math.floor((vaccineDate - birthDate) / (1000 * 60 * 60 * 24));
                    const yearsAtVaccine = Math.floor(ageDaysAtVaccine / 365);
                    const monthsAtVaccine = Math.floor((ageDaysAtVaccine % 365) / 30);
                    currentAgeText = `${yearsAtVaccine}a ${monthsAtVaccine}m`;
                }

                // Statut
                let status = 'status-missing';
                let statusText = '○';
                if (currentVaccination) {
                    if (this.validateVaccineLogic(vaccineName, currentVaccination)) {
                        status = 'status-ok';
                        statusText = '✓';
                    } else {
                        status = 'status-late';
                        statusText = '!';
                    }
                }

                // Date recommandée avec calcul séquentiel
                const recommendedDate = this.calculateRecommendedDate(vaccineName, config, birthDate, proposedDates);
                proposedDates[vaccineName] = recommendedDate; // Stocker la date proposée
                const recommendedDateCell = recommendedDate.toLocaleDateString('fr-FR');

                // Observations
                let observations = '';
                if (!currentVaccination) {
                    if (recommendedDate <= currentDate) {
                        observations = 'À faire maintenant';
                    } else {
                        observations = 'À programmer';
                    }
                } else if (!this.validateVaccineLogic(vaccineName, currentVaccination)) {
                    observations = 'Date invalide.';
                } else {
                    observations = 'Fait - Conforme';
                }

                schemaHTML += `
                    <tr>
                        <td style="border: 1px solid #ddd; padding: 6px;">${nameCell}</td>
                        <td style="border: 1px solid #ddd; padding: 6px;">${currentDateCell}</td>
                        <td style="border: 1px solid #ddd; padding: 6px;">${currentAgeText}</td>
                        <td style="border: 1px solid #ddd; padding: 6px; text-align: center;"><span class="${status}">${statusText}</span></td>
                        <td style="border: 1px solid #ddd; padding: 6px;">${recommendedDateCell}</td>
                        <td style="border: 1px solid #ddd; padding: 6px;">${observations}</td>
                    </tr>
                `;
            });

            schemaHTML += `
                    </tbody>
                </table>
            `;
        });

        schemaHTML += `
                <div class="legend" style="margin-top: 20px; padding-top: 10px; border-top: 2px solid #333;">
                    <strong>Légende:</strong> ✓ = Fait et conforme | ○ = Non fait | ! = Date invalide
                </div>

                <div class="recommendations" style="margin-top: 20px;">
                    <h4>Recommandations importantes:</h4>
                    <ul>
                        <li>Respecter les intervalles minimum entre doses</li>
                        <li>Vérifier les contre-indications avant chaque injection</li>
                        <li>Ce schéma est basé sur les recommandations officielles</li>
                        <li>Consulter un professionnel de santé pour validation</li>
                    </ul>
                </div>
            </div>
        `;

        content.innerHTML = schemaHTML;
        modal.style.display = 'block';
    }

    calculateRecommendedDate(vaccineName, config, birthDate, proposedDates = {}) {
        const minAgeDate = new Date(birthDate.getTime() + (config.minAgeDays * 24 * 60 * 60 * 1000));

        // Si dépend d'un autre vaccin
        if (config.dependsOn) {
            // D'abord vérifier si le vaccin précédent est déjà fait
            const prevVaccineDate = this.data.vaccinations[config.dependsOn];

            if (prevVaccineDate) {
                // Vaccin précédent fait - utiliser sa date réelle
                const prevDate = new Date(prevVaccineDate);
                const minIntervalDate = new Date(prevDate.getTime() + (config.minIntervalDays * 24 * 60 * 60 * 1000));
                return minIntervalDate > minAgeDate ? minIntervalDate : minAgeDate;
            } else if (proposedDates[config.dependsOn]) {
                // Vaccin précédent pas fait mais date proposée disponible
                const prevProposedDate = new Date(proposedDates[config.dependsOn]);
                const minIntervalDate = new Date(prevProposedDate.getTime() + (config.minIntervalDays * 24 * 60 * 60 * 1000));
                return minIntervalDate > minAgeDate ? minIntervalDate : minAgeDate;
            } else {
                // Vaccin précédent pas fait et pas de date proposée - calculer sa date d'abord
                const prevConfig = this.vaccineConfig[config.dependsOn];
                if (prevConfig) {
                    const prevRecommendedDate = this.calculateRecommendedDate(config.dependsOn, prevConfig, birthDate, proposedDates);
                    proposedDates[config.dependsOn] = prevRecommendedDate;
                    const minIntervalDate = new Date(prevRecommendedDate.getTime() + (config.minIntervalDays * 24 * 60 * 60 * 1000));
                    return minIntervalDate > minAgeDate ? minIntervalDate : minAgeDate;
                }
            }
        }

        return minAgeDate > new Date() ? minAgeDate : new Date();
    }

    closeModal(modal) {
        modal.style.display = 'none';
    }

    exportSchema() {
        this.collectData();

        if (!this.data.nomEnfant && !this.data.prenomEnfant) {
            this.showMessage("⚠️ Nom de l'enfant requis pour exporter le schéma");
            return;
        }

        if (!this.data.dateNaissance) {
            this.showMessage("⚠️ Date de naissance requise pour exporter le schéma");
            return;
        }

        // Générer le schéma complet avec recommandations organisées
        const schemaHTML = this.generateExportHTML();

        const blob = new Blob([schemaHTML], { type: 'text/html' });
        const url = URL.createObjectURL(blob);

        const a = document.createElement('a');
        a.href = url;
        a.download = `schema_vaccination_${this.data.prenomEnfant || 'enfant'}_${new Date().toISOString().split('T')[0]}.html`;
        a.click();

        URL.revokeObjectURL(url);
        this.showMessage("💾 Schéma exporté avec succès");
    }

    generateExportHTML() {
        const birthDate = new Date(this.data.dateNaissance);
        const currentDate = new Date();
        const ageDays = Math.floor((currentDate - birthDate) / (1000 * 60 * 60 * 24));
        const years = Math.floor(ageDays / 365);
        const months = Math.floor((ageDays % 365) / 30);

        // Organiser les vaccins par catégorie
        const categories = {
            'Naissance': [],
            'Primo-vaccination': [],
            'Rappel': [],
            'Pneumocoque': [],
            'Rougeole': []
        };

        Object.entries(this.vaccineConfig).forEach(([vaccineName, config]) => {
            categories[config.category].push([vaccineName, config]);
        });

        // Générer les recommandations par catégorie avec calcul séquentiel
        let recommendationsHTML = '';
        const proposedDates = {}; // Pour stocker les dates proposées

        Object.entries(categories).forEach(([category, vaccines]) => {
            let categoryHTML = `<h3>${category}</h3><ul>`;
            let hasRecommendations = false;

            vaccines.forEach(([vaccineName, config]) => {
                const currentVaccination = this.data.vaccinations[vaccineName];
                const isValid = currentVaccination ? this.validateVaccineLogic(vaccineName, currentVaccination) : false;

                if (!currentVaccination || !isValid) {
                    const recommendedDate = this.calculateRecommendedDate(vaccineName, config, birthDate, proposedDates);
                    proposedDates[vaccineName] = recommendedDate; // Stocker la date proposée
                    const status = !currentVaccination ? 'Non fait' : 'Date invalide.';
                    const urgency = recommendedDate <= currentDate ? 'À faire maintenant' : 'À programmer';

                    categoryHTML += `
                        <li>
                            <strong>${config.icon} ${vaccineName}</strong><br>
                            <em>Statut:</em> ${status}<br>
                            <em>Recommandé:</em> ${recommendedDate.toLocaleDateString('fr-FR')} (${urgency})<br>
                            <em>Note:</em> ${config.message}
                        </li>`;
                    hasRecommendations = true;
                } else {
                    categoryHTML += `
                        <li>
                            <strong>${config.icon} ${vaccineName}</strong><br>
                            <em>Statut:</em> ✅ Fait le ${currentVaccination} - Conforme<br>
                            <em>Note:</em> ${config.message}
                        </li>`;
                }
            });

            categoryHTML += '</ul>';

            if (hasRecommendations || vaccines.some(([name]) => this.data.vaccinations[name])) {
                recommendationsHTML += categoryHTML;
            }
        });

        return `<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Schéma de Vaccination - ${this.data.prenomEnfant} ${this.data.nomEnfant}</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 30px;
            line-height: 1.6;
            color: #2c3e50;
            background-color: #f8f9fa;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 0 20px rgba(0,0,0,0.1);
        }
        .header {
            text-align: center;
            border-bottom: 3px solid #3498db;
            padding-bottom: 20px;
            margin-bottom: 30px;
        }
        .header h1 {
            color: #2c3e50;
            margin-bottom: 10px;
        }
        .patient-info {
            background: #e3f2fd;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 30px;
        }
        .patient-info p {
            margin: 8px 0;
        }
        h2, h3, h4 {
            color: #2c3e50;
        }
        ul {
            margin-left: 20px;
        }
        li {
            margin-bottom: 15px;
            padding: 15px;
            background: #f8f9fa;
            border-radius: 5px;
            border-left: 4px solid #3498db;
        }
        .notes {
            background: #fff3e0;
            padding: 20px;
            border-radius: 8px;
            margin-top: 30px;
        }
        .footer {
            text-align: center;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #eee;
            color: #7f8c8d;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🩺 Schéma de Vaccination</h1>
            <p>Application Web - Schéma de Rattrapage Vaccinal pour l'Enfant</p>
        </div>

        <div class="patient-info">
            <h2>📋 Informations de l'Enfant</h2>
            <p><strong>Enfant:</strong> ${this.data.prenomEnfant} ${this.data.nomEnfant}</p>
            <p><strong>Date de naissance:</strong> ${birthDate.toLocaleDateString('fr-FR')}</p>
            <p><strong>Âge:</strong> ${years} ans, ${months} mois</p>
            ${this.data.conditions.poidsInf2000 ? '<p><strong>⚠️ Condition:</strong> Poids < 2000g</p>' : ''}
            ${this.data.conditions.mereHbsPositif ? '<p><strong>⚠️ Condition:</strong> Mère HBs positif</p>' : ''}
        </div>

        <h2>📋 Schéma de Vaccination Détaillé</h2>
        ${recommendationsHTML}

        <div class="notes">
            <h4>📝 Notes Importantes</h4>
            <ul>
                <li>Ce schéma est basé sur les recommandations officielles </li>
                <li>Consultez toujours un professionnel de santé avant toute vaccination</li>
                <li>Respectez les intervalles minimum entre les doses</li>
                <li>Vérifiez les contre-indications spécifiques à chaque vaccin</li>
                <li>En cas de doute, contactez votre médecin traitant</li>
            </ul>
        </div>

        <div class="footer">
            <p>Document généré par l'Application Web - Schéma de Rattrapage Vaccinal</p>
            <p>Créé par Dr. DAOUDI</p>
        </div>
    </div>
</body>
</html>`;
    }

    printSchema() {
        // Créer une nouvelle fenêtre avec le contenu optimisé pour l'impression
        const printWindow = window.open('', '_blank');

        // Créer le document de manière sécurisée avec DOM
        this.createSecurePrintDocument(printWindow);

        // Attendre que le contenu soit chargé avant d'imprimer
        setTimeout(() => {
            printWindow.print();
            // Fermer la fenêtre après impression
            setTimeout(() => {
                printWindow.close();
            }, 1000);
        }, 500);
    }

    createSecurePrintDocument(printWindow) {
        const doc = printWindow.document;

        // Créer la structure HTML de base
        doc.documentElement.setAttribute('lang', 'fr');

        // Créer et configurer le head
        const head = doc.head;
        head.innerHTML = ''; // Vider le head existant

        const metaCharset = doc.createElement('meta');
        metaCharset.setAttribute('charset', 'UTF-8');
        head.appendChild(metaCharset);

        const title = doc.createElement('title');
        title.textContent = `Schéma de Vaccination - ${this.data.prenomEnfant} ${this.data.nomEnfant}`;
        head.appendChild(title);

        // Ajouter les styles CSS de manière sécurisée
        const style = doc.createElement('style');
        style.textContent = this.getPrintStyles();
        head.appendChild(style);

        // Créer le body avec le contenu
        const body = doc.body;
        body.innerHTML = ''; // Vider le body existant

        // Créer le contenu du document
        this.createPrintContent(doc, body);
    }

    getPrintStyles() {
        return `
            @page {
                size: A4 landscape;
                margin: 15mm;
            }
            
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            body {
                font-family: Arial, sans-serif;
                font-size: 12px;
                line-height: 1.3;
                color: black;
                background: white;
            }
            
            .schema-header {
                text-align: center;
                margin-bottom: 12px;
                border-bottom: 2px solid black;
                padding-bottom: 8px;
            }
            
            .schema-header h1 {
                font-size: 18px;
                font-weight: bold;
                margin-bottom: 4px;
            }
            
            .patient-info {
                font-size: 13px;
                margin-bottom: 4px;
            }
            
            .vaccination-table {
                width: 100%;
                border-collapse: collapse;
                margin-top: 8px;
                font-size: 11px;
            }
            
            .vaccination-table th,
            .vaccination-table td {
                border: 1px solid black;
                padding: 4px 6px;
                text-align: left;
                vertical-align: top;
            }
            
            .vaccination-table th {
                background-color: #f0f0f0;
                font-weight: bold;
                font-size: 9px;
            }
            
            .legend {
                margin-top: 10px;
                font-size: 14px;
                border-top: 2px solid black;
                padding-top: 6px;
                font-weight: bold;
            }
            
            .legend-item {
                display: inline-block;
                margin-right: 20px;
            }
            
            .recommendations {
                margin-top: 10px;
                font-size: 12px;
                border-top: 2px solid black;
                padding-top: 6px;
            }
            
            .recommendations h4 {
                font-size: 14px;
                margin-bottom: 5px;
                font-weight: bold;
            }
            
            .recommendations ul {
                margin-left: 15px;
            }
            
            .recommendations li {
                margin-bottom: 2px;
                line-height: 1.4;
            }
        `;
    }

    createPrintContent(doc, body) {
        const birthDate = new Date(this.data.dateNaissance);
        const currentDate = new Date();
        const ageDays = Math.floor((currentDate - birthDate) / (1000 * 60 * 60 * 24));
        const years = Math.floor(ageDays / 365);
        const months = Math.floor((ageDays % 365) / 30);

        // Créer l'en-tête
        const header = doc.createElement('div');
        header.className = 'schema-header';

        const h1 = doc.createElement('h1');
        h1.textContent = 'SCHÉMA DE RATTRAPAGE VACCINAL';
        header.appendChild(h1);

        const patientInfo = doc.createElement('div');
        patientInfo.className = 'patient-info';

        const enfantDiv = doc.createElement('div');
        const enfantStrong = doc.createElement('strong');
        enfantStrong.textContent = 'Enfant: ';
        enfantDiv.appendChild(enfantStrong);
        enfantDiv.appendChild(doc.createTextNode(`${this.data.prenomEnfant} ${this.data.nomEnfant}`));
        patientInfo.appendChild(enfantDiv);

        const naissanceDiv = doc.createElement('div');
        const naissanceStrong = doc.createElement('strong');
        naissanceStrong.textContent = 'Né(e) le: ';
        naissanceDiv.appendChild(naissanceStrong);
        naissanceDiv.appendChild(doc.createTextNode(`${birthDate.toLocaleDateString('fr-FR')} (${years} ans, ${months} mois)`));
        patientInfo.appendChild(naissanceDiv);

        const genereDiv = doc.createElement('div');
        const genereStrong = doc.createElement('strong');
        genereStrong.textContent = 'Généré le: ';
        genereDiv.appendChild(genereStrong);
        genereDiv.appendChild(doc.createTextNode(`${currentDate.toLocaleDateString('fr-FR')} `));
        patientInfo.appendChild(genereDiv);

        if (this.data.conditions.poidsInf2000) {
            const poidsDiv = doc.createElement('div');
            const poidsStrong = doc.createElement('strong');
            poidsStrong.textContent = '⚠️ Condition: ';
            poidsDiv.appendChild(poidsStrong);
            poidsDiv.appendChild(doc.createTextNode('Poids < 2000g'));
            patientInfo.appendChild(poidsDiv);
        }

        if (this.data.conditions.mereHbsPositif) {
            const hbsDiv = doc.createElement('div');
            const hbsStrong = doc.createElement('strong');
            hbsStrong.textContent = '⚠️ Condition: ';
            hbsDiv.appendChild(hbsStrong);
            hbsDiv.appendChild(doc.createTextNode('Mère HBs positif'));
            patientInfo.appendChild(hbsDiv);
        }

        header.appendChild(patientInfo);
        body.appendChild(header);

        // Créer le tableau de vaccination
        const table = doc.createElement('table');
        table.className = 'vaccination-table';

        // En-tête du tableau
        const thead = doc.createElement('thead');
        const headerRow = doc.createElement('tr');

        const headers = ['Vaccin', 'Date Actuelle', 'Âge Actuel', 'Statut', 'Date Recommandée', 'Observations'];
        headers.forEach(headerText => {
            const th = doc.createElement('th');
            th.textContent = headerText;
            headerRow.appendChild(th);
        });

        thead.appendChild(headerRow);
        table.appendChild(thead);

        // Corps du tableau
        const tbody = doc.createElement('tbody');

        // Organiser les vaccins par catégorie
        const categories = {
            'Naissance': [],
            'Primo-vaccination': [],
            'Rappel': [],
            'Pneumocoque': [],
            'Rougeole': []
        };

        Object.entries(this.vaccineConfig).forEach(([vaccineName, config]) => {
            categories[config.category].push([vaccineName, config]);
        });

        // Générer les lignes du tableau avec calcul séquentiel des dates
        const proposedDates = {}; // Pour stocker les dates proposées

        Object.entries(categories).forEach(([category, vaccines]) => {
            // Ligne de catégorie
            const categoryRow = doc.createElement('tr');
            const categoryCell = doc.createElement('td');
            categoryCell.textContent = category;
            categoryCell.colSpan = '6';
            categoryCell.style.backgroundColor = '#f0f0f0';
            categoryCell.style.fontWeight = 'bold';
            categoryCell.style.textAlign = 'center';
            categoryRow.appendChild(categoryCell);
            tbody.appendChild(categoryRow);

            // Lignes des vaccins
            vaccines.forEach(([vaccineName, config]) => {
                const row = doc.createElement('tr');

                // Nom du vaccin
                const nameCell = doc.createElement('td');
                nameCell.textContent = `${config.icon} ${vaccineName}`;

                // Date actuelle
                const currentVaccination = this.data.vaccinations[vaccineName];
                const currentDateCell = doc.createElement('td');
                currentDateCell.textContent = currentVaccination || '-';

                // Âge actuel lors de la vaccination
                let currentAgeText = '-';
                if (currentVaccination) {
                    const vaccineDate = new Date(currentVaccination);
                    const ageDaysAtVaccine = Math.floor((vaccineDate - birthDate) / (1000 * 60 * 60 * 24));
                    const yearsAtVaccine = Math.floor(ageDaysAtVaccine / 365);
                    const monthsAtVaccine = Math.floor((ageDaysAtVaccine % 365) / 30);
                    currentAgeText = `${yearsAtVaccine}a ${monthsAtVaccine}m`;
                }
                const currentAgeCell = doc.createElement('td');
                currentAgeCell.textContent = currentAgeText;

                // Statut
                let status = 'status-missing';
                let statusText = '○';
                if (currentVaccination) {
                    if (this.validateVaccineLogic(vaccineName, currentVaccination)) {
                        status = 'status-ok';
                        statusText = '✓';
                    } else {
                        status = 'status-late';
                        statusText = '!';
                    }
                }
                const statusCell = doc.createElement('td');
                statusCell.textContent = statusText;
                statusCell.style.textAlign = 'center';

                // Date recommandée avec calcul séquentiel
                const recommendedDate = this.calculateRecommendedDate(vaccineName, config, birthDate, proposedDates);
                proposedDates[vaccineName] = recommendedDate; // Stocker la date proposée
                const recommendedDateCell = doc.createElement('td');
                recommendedDateCell.textContent = recommendedDate.toLocaleDateString('fr-FR');

                // Observations
                let observations = '';
                if (!currentVaccination) {
                    if (recommendedDate <= currentDate) {
                        observations = 'À faire maintenant';
                    } else {
                        observations = 'À programmer';
                    }
                } else if (!this.validateVaccineLogic(vaccineName, currentVaccination)) {
                    observations = 'Date invalide.';
                } else {
                    observations = 'Fait - Conforme';
                }
                const observationsCell = doc.createElement('td');
                observationsCell.textContent = observations;

                row.appendChild(nameCell);
                row.appendChild(currentDateCell);
                row.appendChild(currentAgeCell);
                row.appendChild(statusCell);
                row.appendChild(recommendedDateCell);
                row.appendChild(observationsCell);

                tbody.appendChild(row);
            });
        });

        table.appendChild(tbody);
        body.appendChild(table);

        // Légende
        const legend = doc.createElement('div');
        legend.className = 'legend';
        legend.innerHTML = '<strong>Légende:</strong> ✓ = Fait et conforme | ○ = Non fait | ! = Date invalide';
        body.appendChild(legend);

        // Recommandations
        const recommendations = doc.createElement('div');
        recommendations.className = 'recommendations';
        const recTitle = doc.createElement('h4');
        recTitle.textContent = 'Recommandations importantes:';
        recommendations.appendChild(recTitle);
        const recList = doc.createElement('ul');

        const recItems = [
            'Respecter les intervalles minimum entre doses',
            'Vérifier les contre-indications avant chaque injection',
            'Ce schéma est basé sur les recommandations officielles',
            'Consulter un professionnel de santé pour validation'
        ];

        recItems.forEach(item => {
            const li = doc.createElement('li');
            li.textContent = item;
            recList.appendChild(li);
        });

        recommendations.appendChild(recList);
        body.appendChild(recommendations);
    }

    // Fonctions centrales pour mettre à jour tous les statuts
    updateAllStatuses() {
        // Forcer la collecte des données avant mise à jour
        this.collectData();

        // Calculer les statuts manuellement pour s'assurer qu'ils sont corrects
        let validCount = 0;
        let invalidCount = 0;
        let missingCount = 0;
        let vaccinsAJour = 0;
        let vaccinsRetard = 0;

        Object.keys(this.vaccineConfig).forEach(vaccineName => {
            const date = this.data.vaccinations[vaccineName];

            if (date) {
                if (this.validateVaccineLogic(vaccineName, date)) {
                    validCount++;
                    vaccinsAJour++;
                } else {
                    invalidCount++;
                }
            } else {
                missingCount++;
                // Vérifier si le vaccin est en retard
                if (this.data.dateNaissance) {
                    const config = this.vaccineConfig[vaccineName];
                    const birthDate = new Date(this.data.dateNaissance);
                    const recommendedDate = this.calculateRecommendedDate(vaccineName, config, birthDate);

                    if (recommendedDate <= new Date()) {
                        vaccinsRetard++;
                    }
                }
            }
        });

        // Mettre à jour l'affichage directement
        this.updateDisplayElements(validCount, invalidCount, missingCount, vaccinsAJour, vaccinsRetard);
    }

    // Fonction pour mettre à jour les éléments d'affichage
    updateDisplayElements(validCount, invalidCount, missingCount, vaccinsAJour, vaccinsRetard) {
        // Résumé rapide (onglet Vaccinations)
        const quickValidElement = document.getElementById('quickValidCount');
        const quickInvalidElement = document.getElementById('quickInvalidCount');
        const quickMissingElement = document.getElementById('quickMissingCount');

        if (quickValidElement) quickValidElement.textContent = validCount;
        if (quickInvalidElement) quickInvalidElement.textContent = invalidCount;
        if (quickMissingElement) quickMissingElement.textContent = missingCount;

        // Statut détaillé (onglet Actions)
        const vaccinsAJourElement = document.getElementById('vaccinsAJour');
        const vaccinsRetardElement = document.getElementById('vaccinsRetard');
        const prochainsVaccinsElement = document.getElementById('prochainsVaccins');

        if (vaccinsAJourElement) vaccinsAJourElement.textContent = vaccinsAJour;
        if (vaccinsRetardElement) vaccinsRetardElement.textContent = vaccinsRetard;

        // Calculer le prochain vaccin
        if (prochainsVaccinsElement && this.data.dateNaissance) {
            const prochainsVaccins = [];
            Object.keys(this.vaccineConfig).forEach(vaccineName => {
                const date = this.data.vaccinations[vaccineName];
                if (!date) {
                    const config = this.vaccineConfig[vaccineName];
                    const birthDate = new Date(this.data.dateNaissance);
                    const recommendedDate = this.calculateRecommendedDate(vaccineName, config, birthDate);

                    if (recommendedDate > new Date()) {
                        prochainsVaccins.push({
                            name: vaccineName,
                            date: recommendedDate
                        });
                    }
                }
            });

            prochainsVaccins.sort((a, b) => a.date - b.date);
            const prochainVaccin = prochainsVaccins.length > 0 ?
                prochainsVaccins[0].date.toLocaleDateString('fr-FR') : '-';
            prochainsVaccinsElement.textContent = prochainVaccin;
        }
    }

    updateVaccinationStatus() {
        // S'assurer que les données sont à jour
        this.collectData();

        let vaccinsAJour = 0;
        let vaccinsRetard = 0;
        let prochainsVaccins = [];

        Object.keys(this.vaccineConfig).forEach(vaccineName => {
            const date = this.data.vaccinations[vaccineName];
            if (date && this.validateVaccineLogic(vaccineName, date)) {
                vaccinsAJour++;
            } else if (!date && this.data.dateNaissance) {
                const config = this.vaccineConfig[vaccineName];
                const birthDate = new Date(this.data.dateNaissance);
                const recommendedDate = this.calculateRecommendedDate(vaccineName, config, birthDate);

                if (recommendedDate <= new Date()) {
                    vaccinsRetard++;
                } else {
                    prochainsVaccins.push({
                        name: vaccineName,
                        date: recommendedDate
                    });
                }
            }
        });

        // Trier les prochains vaccins par date
        prochainsVaccins.sort((a, b) => a.date - b.date);
        const prochainVaccin = prochainsVaccins.length > 0 ?
            prochainsVaccins[0].date.toLocaleDateString('fr-FR') : '-';

        // Mettre à jour l'affichage avec vérification d'existence des éléments
        const vaccinsAJourElement = document.getElementById('vaccinsAJour');
        const vaccinsRetardElement = document.getElementById('vaccinsRetard');
        const prochainsVaccinsElement = document.getElementById('prochainsVaccins');

        if (vaccinsAJourElement) vaccinsAJourElement.textContent = vaccinsAJour;
        if (vaccinsRetardElement) vaccinsRetardElement.textContent = vaccinsRetard;
        if (prochainsVaccinsElement) prochainsVaccinsElement.textContent = prochainVaccin;
    }

    updateQuickSummary() {
        // S'assurer que les données sont à jour
        this.collectData();

        let validCount = 0;
        let invalidCount = 0;
        let missingCount = 0;

        Object.keys(this.vaccineConfig).forEach(vaccineName => {
            const date = this.data.vaccinations[vaccineName];
            if (date) {
                if (this.validateVaccineLogic(vaccineName, date)) {
                    validCount++;
                } else {
                    invalidCount++;
                }
            } else {
                missingCount++;
            }
        });

        // Mettre à jour l'affichage du résumé rapide
        const quickValidElement = document.getElementById('quickValidCount');
        const quickInvalidElement = document.getElementById('quickInvalidCount');
        const quickMissingElement = document.getElementById('quickMissingCount');

        if (quickValidElement) quickValidElement.textContent = validCount;
        if (quickInvalidElement) quickInvalidElement.textContent = invalidCount;
        if (quickMissingElement) quickMissingElement.textContent = missingCount;
    }

    showMessage(message, type) {
        const messageArea = document.getElementById('messageArea');
        if (messageArea) {
            // Add type-specific styling
            let prefix = '';
            switch(type) {
                case 'success':
                    prefix = '✅ ';
                    break;
                case 'error':
                    prefix = '❌ ';
                    break;
                case 'warning':
                    prefix = '⚠️ ';
                    break;
                default:
                    prefix = 'ℹ️ ';
            }
            
            messageArea.textContent = prefix + message;
        }
    }
}

// Initialiser l'application quand le DOM est chargé
document.addEventListener('DOMContentLoaded', function() {
    window.vaccinationApp = new VaccinationApp();
});