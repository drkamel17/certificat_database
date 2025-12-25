// Gestionnaire pour le formulaire de récupération de données
document.addEventListener('DOMContentLoaded', function() {
    const form = document.getElementById('dateForm');
    const loading = document.getElementById('loading');
    const successMessage = document.getElementById('successMessage');
    const errorMessage = document.getElementById('errorMessage');
    const results = document.getElementById('results');
    const resultsContent = document.getElementById('resultsContent');
    const exportExcelBtn = document.getElementById('exportExcel');
    const printTableBtn = document.getElementById('printTable');
    
    let currentData = [];
    let currentPage = 1;
    let rowsPerPage = 20;
    window.currentRowsPerPage = rowsPerPage; // Rendre accessible globalement
    
    // Gérer la soumission du formulaire
    form.addEventListener('submit', async function(e) {
        e.preventDefault();
        console.log('Formulaire soumis');
        
        const table = document.getElementById('table').value;
        const dateDebut = document.getElementById('dateDebut').value;
        const dateFin = document.getElementById('dateFin').value;
        
        console.log(`Valeurs du formulaire - Table: ${table}, Début: ${dateDebut}, Fin: ${dateFin}`);
        
        if (!table || !dateDebut || !dateFin) {
            console.error('Champs manquants');
            showError('Veuillez remplir tous les champs');
            return;
        }
        
        // Valider que la date de fin est après la date de début
        if (new Date(dateFin) < new Date(dateDebut)) {
            console.error('Date de fin avant date de début');
            showError('La date de fin doit être après la date de début');
            return;
        }
        
        console.log('Lancement de la récupération des données');
        await recupererDonnees(table, dateDebut, dateFin);
    });
    
    // Fonction pour récupérer les données
    async function recupererDonnees(table, dateDebut, dateFin) {
        console.log(`Récupération des données - Table: ${table}, Début: ${dateDebut}, Fin: ${dateFin}`);
        
        loading.style.display = 'block';
        successMessage.style.display = 'none';
        errorMessage.style.display = 'none';
        results.style.display = 'none';
        
        try {
            const requestData = {
                table: table,
                date_debut: dateDebut,
                date_fin: dateFin
            };
            
            console.log('Données envoyées:', JSON.stringify(requestData, null, 2));
            
            const response = await fetch('http://127.0.0.1:5000/api/recuperer_donnees', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(requestData)
            });
            
            console.log('Status de la réponse:', response.status);
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const result = await response.json();
            console.log('Résultat reçu:', result);
            
            if (result.success) {
                currentData = result.data;
                console.log(`Données récupérées: ${currentData.length} enregistrements`);
                
                // Nettoyer la recherche avant d'afficher les nouveaux résultats
                if (typeof clearSearch === 'function') {
                    clearSearch();
                }
                
                afficherResultats(result.data, table);
                showSuccess(`${result.returned} enregistrements trouvés sur un total de ${result.total}`);
            } else {
                console.error('Erreur retournée par l\'API:', result.error);
                showError(result.error || 'Erreur lors de la récupération des données');
            }
        } catch (error) {
            console.error('Erreur complète:', error);
            showError('Erreur de connexion au serveur. Vérifiez que le serveur API est démarré.');
        } finally {
            loading.style.display = 'none';
        }
    }
    
    // Fonction pour afficher les résultats avec pagination
    function afficherResultats(data, tableType) {
        if (!data || data.length === 0) {
            resultsContent.innerHTML = '<p>Aucune donnée trouvée pour la période sélectionnée.</p>';
            results.style.display = 'block';
            return;
        }
        
        // Réinitialiser la page courante
        currentPage = 1;
        
        // Créer le conteneur principal
        const container = document.createElement('div');
        container.id = 'resultsContainer';
        
        // Ajouter les informations de pagination
        const paginationInfo = document.createElement('div');
        paginationInfo.style.cssText = 'margin-bottom: 15px; padding: 10px; background-color: #e9ecef; border-radius: 4px; text-align: center;';
        paginationInfo.innerHTML = `
            <strong>Total:</strong> ${data.length} enregistrements | 
            <strong>Page:</strong> <span id="currentPageInfo">1</span> / <span id="totalPagesInfo">${Math.ceil(data.length / rowsPerPage)}</span> | 
            <strong>Affichage:</strong> ${Math.min(rowsPerPage, data.length)} enregistrements
        `;
        container.appendChild(paginationInfo);
        
        // Ajouter les boutons d'action au-dessus du tableau
        const actionButtons = document.createElement('div');
        actionButtons.id = 'actionButtons';
        actionButtons.style.cssText = 'margin-bottom: 15px; display: none; text-align: right;';
        actionButtons.innerHTML = `
            <button id="editBtn" style="background-color: #ffc107; color: black; padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer; margin-right: 10px;">
                <i class="fas fa-edit"></i> Modifier
            </button>
            <button id="deleteBtn" style="background-color: #dc3545; color: white; padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer;">
                <i class="fas fa-trash"></i> Supprimer
            </button>
        `;
        container.appendChild(actionButtons);
        
        // Créer le tableau
        const tableElement = document.createElement('table');
        tableElement.id = 'resultsTable';
        
        // Créer les en-têtes
        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        
        let columns = [];
        if (tableType === 'arrets_travail' || tableType === 'prolongation') {
            columns = ['Nom', 'Prénom', 'Médecin', 'Nombre de jours', 'Date certificat', 'Date naissance', 'Âge', 'Créé le'];
        } else if (tableType === 'cbv') {
            columns = ['Nom', 'Prénom', 'Médecin', 'Date certificat', 'Heure', 'Date naissance', 'Titre', 'Examen', 'Créé le'];
        } else if (tableType === 'antirabique') {
            columns = ['Nom', 'Prénom', 'Médecin', 'Classe', 'Type vaccin', 'Schéma', 'Date certificat', 'Date naissance', 'Animal', 'Créé le'];
        } else if (tableType === 'dece') {
            columns = ['Nom', 'Prénom', 'Date de décès', 'Heure de décès', 'Lieu de décès', 'Cause de décès', 'Médecin'];
        }
        
        columns.forEach(col => {
            const th = document.createElement('th');
            th.textContent = col;
            headerRow.appendChild(th);
        });
        
        thead.appendChild(headerRow);
        tableElement.appendChild(thead);
        
        // Créer le corps du tableau
        const tbody = document.createElement('tbody');
        tbody.id = 'tableBody';
        tableElement.appendChild(tbody);
        
        container.appendChild(tableElement);
        
        // Ajouter la pagination
        const paginationDiv = document.createElement('div');
        paginationDiv.id = 'pagination';
        paginationDiv.style.cssText = 'margin-top: 20px; text-align: center;';
        container.appendChild(paginationDiv);
        
        // Remplacer le contenu
        resultsContent.innerHTML = '';
        resultsContent.appendChild(container);
        
        results.style.display = 'block';
        
        // Afficher la première page
        afficherPage(data, tableType, 1);
        
        // Ajouter les gestionnaires d'événements pour la sélection des lignes
        ajouterGestionnairesSelection(tableType);
    }
    
    // Fonction pour afficher une page spécifique
    function afficherPage(data, tableType, page) {
        const tbody = document.getElementById('tableBody');
        const paginationDiv = document.getElementById('pagination');
        const currentPageInfo = document.getElementById('currentPageInfo');
        const totalPagesInfo = document.getElementById('totalPagesInfo');
        
        // Obtenir le nombre de lignes par page
        const rowsPerPageValue = window.currentRowsPerPage || rowsPerPage;
        
        // Calculer les indices
        const startIndex = (page - 1) * rowsPerPageValue;
        const endIndex = Math.min(startIndex + rowsPerPageValue, data.length);
        const pageData = data.slice(startIndex, endIndex);
        
        // Vider le corps du tableau
        tbody.innerHTML = '';
        
        // Créer les lignes pour cette page
        pageData.forEach((row, index) => {
            const tr = document.createElement('tr');
            tr.dataset.id = row.id;
            tr.dataset.index = startIndex + index;
            
            if (tableType === 'arrets_travail' || tableType === 'prolongation') {
                tr.innerHTML = `
                    <td>${row.nom || ''}</td>
                    <td>${row.prenom || ''}</td>
                    <td>${row.medecin || ''}</td>
                    <td>${row.nombre_jours || ''}</td>
                    <td>${row.date_certificat || ''}</td>
                    <td>${row.date_naissance || ''}</td>
                    <td>${row.age || ''}</td>
                    <td>${row.created_at || ''}</td>
                `;
            } else if (tableType === 'cbv') {
                tr.innerHTML = `
                    <td>${row.nom || ''}</td>
                    <td>${row.prenom || ''}</td>
                    <td>${row.medecin || ''}</td>
                    <td>${row.date_certificat || ''}</td>
                    <td>${row.heure || ''}</td>
                    <td>${row.date_naissance || ''}</td>
                    <td>${row.titre || ''}</td>
                    <td>${row.examen || ''}</td>
                    <td>${row.created_at || ''}</td>
                `;
            } else if (tableType === 'antirabique') {
                tr.innerHTML = `
                    <td>${row.nom || ''}</td>
                    <td>${row.prenom || ''}</td>
                    <td>${row.medecin || ''}</td>
                    <td>${row.classe || ''}</td>
                    <td>${row.type_de_vaccin || ''}</td>
                    <td>${row.shema || ''}</td>
                    <td>${row.date_de_certificat || ''}</td>
                    <td>${row.date_de_naissance || ''}</td>
                    <td>${row.animal || ''}</td>
                    <td>${row.created_at || ''}</td>
                `;
            } else if (tableType === 'dece') {
                tr.innerHTML = `
                    <td>${row.nom || ''}</td>
                    <td>${row.prenom || ''}</td>
                    <td>${row.date_deces || row.dateDeces || ''}</td>
                    <td>${row.heure_deces || row.heureDeces || ''}</td>
                    <td>${row.lieuDeces || ''}</td>
                    <td>${row.causeDeces || ''}</td>
                    <td>${row.medecin || ''}</td>
                `;
            }
            
            tbody.appendChild(tr);
        });
        
        // Mettre à jour les informations de pagination
        currentPage = page;
        const totalPages = Math.ceil(data.length / rowsPerPageValue);
        currentPageInfo.textContent = page;
        totalPagesInfo.textContent = totalPages;
        
        // Créer les boutons de pagination
        creerPagination(data.length, page, tableType);
    }
    
    // Fonction pour créer les contrôles de pagination
    function creerPagination(totalItems, currentPage, tableType) {
        const paginationDiv = document.getElementById('pagination');
        const rowsPerPageValue = window.currentRowsPerPage || rowsPerPage;
        const totalPages = Math.ceil(totalItems / rowsPerPageValue);
        
        let paginationHTML = '<div style="display: inline-flex; align-items: center; gap: 10px;">';
        
        // Bouton précédent
        const prevDisabled = currentPage === 1 ? 'disabled' : '';
        paginationHTML += `
            <button onclick="changePage(${currentPage - 1}, '${tableType}')" 
                    style="padding: 8px 12px; border: 1px solid #ddd; background: white; cursor: pointer; ${prevDisabled ? 'opacity: 0.5; cursor: not-allowed;' : ''}">
                <i class="fas fa-chevron-left"></i> Précédent
            </button>
        `;
        
        // Informations sur la page
        paginationHTML += `
            <span style="padding: 8px 12px; background: #f8f9fa; border: 1px solid #ddd;">
                Page ${currentPage} / ${totalPages}
            </span>
        `;
        
        // Bouton suivant
        const nextDisabled = currentPage === totalPages ? 'disabled' : '';
        paginationHTML += `
            <button onclick="changePage(${currentPage + 1}, '${tableType}')" 
                    style="padding: 8px 12px; border: 1px solid #ddd; background: white; cursor: pointer; ${nextDisabled ? 'opacity: 0.5; cursor: not-allowed;' : ''}">
                Suivant <i class="fas fa-chevron-right"></i>
            </button>
        `;
        
        // Sélecteur de nombre de lignes
        paginationHTML += `
            <select onchange="changeRowsPerPage(this.value, '${tableType}')" 
                    style="padding: 8px; border: 1px solid #ddd; background: white;">
                <option value="10" ${rowsPerPage === 10 ? 'selected' : ''}>10 lignes</option>
                <option value="20" ${rowsPerPage === 20 ? 'selected' : ''}>20 lignes</option>
                <option value="50" ${rowsPerPage === 50 ? 'selected' : ''}>50 lignes</option>
                <option value="100" ${rowsPerPage === 100 ? 'selected' : ''}>100 lignes</option>
            </select>
        `;
        
        paginationHTML += '</div>';
        
        paginationDiv.innerHTML = paginationHTML;
    }
    
    // Fonction globale pour changer de page
    window.changePage = function(page, tableType) {
        const rowsPerPageValue = window.currentRowsPerPage || rowsPerPage;
        if (page < 1 || page > Math.ceil(currentData.length / rowsPerPageValue)) return;
        afficherPage(currentData, tableType, page);
    };
    
    // Fonction globale pour changer le nombre de lignes par page
    window.changeRowsPerPage = function(newRowsPerPage, tableType) {
        // Mettre à jour la variable globale
        window.currentRowsPerPage = parseInt(newRowsPerPage);
        
        // Réafficher la première page
        afficherPage(currentData, tableType, 1);
    };
    
    // Variables globales pour la sélection
    let selectedRow = null;
    let selectedData = null;
    let tableTypeGlobal = null;
    
    // Fonction pour ajouter les gestionnaires de sélection de ligne
    function ajouterGestionnairesSelection(tableType) {
        const tableElement = document.getElementById('resultsTable');
        const actionButtons = document.getElementById('actionButtons');
        const editBtn = document.getElementById('editBtn');
        const deleteBtn = document.getElementById('deleteBtn');
        
        if (!tableElement || !actionButtons) return;
        
        // Mettre à jour le type de table global
        tableTypeGlobal = tableType;
        
        // Supprimer les anciens gestionnaires d'événements s'ils existent
        const newTableElement = tableElement.cloneNode(true);
        tableElement.parentNode.replaceChild(newTableElement, tableElement);
        
        const newEditBtn = editBtn.cloneNode(true);
        editBtn.parentNode.replaceChild(newEditBtn, editBtn);
        
        const newDeleteBtn = deleteBtn.cloneNode(true);
        deleteBtn.parentNode.replaceChild(newDeleteBtn, deleteBtn);
        
        // Gestionnaire de clic sur les lignes du tableau
        newTableElement.addEventListener('click', function(e) {
            const row = e.target.closest('tr');
            if (!row || row.tagName === 'THEAD' || !row.dataset.id) return;
            
            // Désélectionner la ligne précédente
            if (selectedRow) {
                selectedRow.style.backgroundColor = '';
            }
            
            // Sélectionner la nouvelle ligne
            selectedRow = row;
            row.style.backgroundColor = '#e3f2fd';
            
            // Récupérer les données de la ligne
            const index = parseInt(row.dataset.index);
            selectedData = currentData[index];
            
            // Afficher les boutons d'action
            actionButtons.style.display = 'block';
        });
        
        // Gestionnaire pour le bouton de modification
        newEditBtn.addEventListener('click', function() {
            if (selectedData) {
                modifierEnregistrement(selectedData, tableTypeGlobal);
            }
        });
        
        // Gestionnaire pour le bouton de suppression
        newDeleteBtn.addEventListener('click', function() {
            if (selectedData) {
                supprimerEnregistrement(selectedData, tableTypeGlobal);
            }
        });
    }
    
    // Gestionnaire global pour cliquer en dehors du tableau
    document.addEventListener('click', function(e) {
        const tableElement = document.getElementById('resultsTable');
        const actionButtons = document.getElementById('actionButtons');
        
        if (tableElement && actionButtons && 
            !tableElement.contains(e.target) && !actionButtons.contains(e.target)) {
            if (selectedRow) {
                selectedRow.style.backgroundColor = '';
                selectedRow = null;
                selectedData = null;
                actionButtons.style.display = 'none';
            }
        }
    });
    
    // Fonction pour modifier un enregistrement
    async function modifierEnregistrement(data, table) {
        // Créer une boîte de dialogue modale pour la modification
        const modal = document.createElement('div');
        modal.style.cssText = `
            position: fixed; top: 0; left: 0; width: 100%; height: 100%; 
            background-color: rgba(0,0,0,0.5); z-index: 1000; display: flex; 
            align-items: center; justify-content: center;
        `;
        
        let formContent = '';
        
        // Générer le formulaire selon le type de table
        if (table === 'arrets_travail' || table === 'prolongation') {
            formContent = `
                <h3>Modifier l'enregistrement</h3>
                <div class="form-group">
                    <label>Nom:</label>
                    <input type="text" id="editNom" value="${data.nom || ''}" required>
                </div>
                <div class="form-group">
                    <label>Prénom:</label>
                    <input type="text" id="editPrenom" value="${data.prenom || ''}" required>
                </div>
                <div class="form-group">
                    <label>Médecin:</label>
                    <input type="text" id="editMedecin" value="${data.medecin || ''}" required>
                </div>
                <div class="form-group">
                    <label>Nombre de jours:</label>
                    <input type="number" id="editNombreJours" value="${data.nombre_jours || ''}" required>
                </div>
                <div class="form-group">
                    <label>Date certificat:</label>
                    <input type="date" id="editDateCertificat" value="${data.date_certificat || ''}" required>
                </div>
                <div class="form-group">
                    <label>Date naissance:</label>
                    <input type="text" id="editDateNaissance" value="${data.date_naissance || ''}">
                </div>
                <div class="form-group">
                    <label>Âge:</label>
                    <input type="number" id="editAge" value="${data.age || ''}">
                </div>
            `;
        } else if (table === 'cbv') {
            formContent = `
                <h3>Modifier le certificat CBV</h3>
                <div class="form-group">
                    <label>Nom:</label>
                    <input type="text" id="editNom" value="${data.nom || ''}" required>
                </div>
                <div class="form-group">
                    <label>Prénom:</label>
                    <input type="text" id="editPrenom" value="${data.prenom || ''}" required>
                </div>
                <div class="form-group">
                    <label>Médecin:</label>
                    <input type="text" id="editMedecin" value="${data.medecin || ''}" required>
                </div>
                <div class="form-group">
                    <label>Date certificat:</label>
                    <input type="date" id="editDateCertificat" value="${data.date_certificat || ''}" required>
                </div>
                <div class="form-group">
                    <label>Heure:</label>
                    <input type="text" id="editHeure" value="${data.heure || ''}">
                </div>
                <div class="form-group">
                    <label>Date naissance:</label>
                    <input type="text" id="editDateNaissance" value="${data.date_naissance || ''}">
                </div>
                <div class="form-group">
                    <label>Titre:</label>
                    <input type="text" id="editTitre" value="${data.titre || ''}">
                </div>
                <div class="form-group">
                    <label>Examen:</label>
                    <input type="text" id="editExamen" value="${data.examen || ''}">
                </div>
            `;
        } else if (table === 'antirabique') {
            formContent = `
                <h3>Modifier le certificat antirabique</h3>
                <div class="form-group">
                    <label>Nom:</label>
                    <input type="text" id="editNom" value="${data.nom || ''}" required>
                </div>
                <div class="form-group">
                    <label>Prénom:</label>
                    <input type="text" id="editPrenom" value="${data.prenom || ''}" required>
                </div>
                <div class="form-group">
                    <label>Médecin:</label>
                    <input type="text" id="editMedecin" value="${data.medecin || ''}" required>
                </div>
                <div class="form-group">
                    <label>Classe:</label>
                    <input type="text" id="editClasse" value="${data.classe || ''}">
                </div>
                <div class="form-group">
                    <label>Type de vaccin:</label>
                    <input type="text" id="editTypeDeVaccin" value="${data.type_de_vaccin || ''}">
                </div>
                <div class="form-group">
                    <label>Schéma:</label>
                    <input type="text" id="editShema" value="${data.shema || ''}">
                </div>
                <div class="form-group">
                    <label>Date certificat:</label>
                    <input type="date" id="editDateCertificat" value="${data.date_de_certificat || ''}" required>
                </div>
                <div class="form-group">
                    <label>Date naissance:</label>
                    <input type="text" id="editDateNaissance" value="${data.date_de_naissance || ''}">
                </div>
                <div class="form-group">
                    <label>Animal:</label>
                    <input type="text" id="editAnimal" value="${data.animal || ''}">
                </div>
            `;
        }
        
        modal.innerHTML = `
            <div style="background-color: white; padding: 20px; border-radius: 8px; max-width: 500px; width: 90%; max-height: 80vh; overflow-y: auto;">
                ${formContent}
                <div style="margin-top: 20px; text-align: right;">
                    <button id="cancelEdit" style="background-color: #6c757d; color: white; padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer; margin-right: 10px;">Annuler</button>
                    <button id="saveEdit" style="background-color: #007bff; color: white; padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer;">Enregistrer</button>
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
        
        // Gestionnaires d'événements pour la modale
        document.getElementById('cancelEdit').addEventListener('click', function() {
            document.body.removeChild(modal);
        });
        
        document.getElementById('saveEdit').addEventListener('click', async function() {
            await sauvegarderModification(data.id, table, modal);
        });
        
        // Fermer la modale en cliquant à l'extérieur
        modal.addEventListener('click', function(e) {
            if (e.target === modal) {
                document.body.removeChild(modal);
            }
        });
    }
    
    // Fonction pour sauvegarder la modification
    async function sauvegarderModification(id, table, modal) {
        try {
            let updateData = { id: id };
            
            // Récupérer les données selon le type de table
            if (table === 'arrets_travail' || table === 'prolongation') {
                updateData = {
                    id: id,
                    nom: document.getElementById('editNom').value,
                    prenom: document.getElementById('editPrenom').value,
                    medecin: document.getElementById('editMedecin').value,
                    nombre_jours: parseInt(document.getElementById('editNombreJours').value),
                    date_certificat: document.getElementById('editDateCertificat').value,
                    date_naissance: document.getElementById('editDateNaissance').value || null,
                    age: parseInt(document.getElementById('editAge').value) || null
                };
            } else if (table === 'cbv') {
                updateData = {
                    id: id,
                    nom: document.getElementById('editNom').value,
                    prenom: document.getElementById('editPrenom').value,
                    medecin: document.getElementById('editMedecin').value,
                    date_certificat: document.getElementById('editDateCertificat').value,
                    heure: document.getElementById('editHeure').value || null,
                    date_naissance: document.getElementById('editDateNaissance').value || null,
                    titre: document.getElementById('editTitre').value || null,
                    examen: document.getElementById('editExamen').value || null
                };
            } else if (table === 'antirabique') {
                updateData = {
                    id: id,
                    nom: document.getElementById('editNom').value,
                    prenom: document.getElementById('editPrenom').value,
                    medecin: document.getElementById('editMedecin').value,
                    classe: document.getElementById('editClasse').value,
                    type_de_vaccin: document.getElementById('editTypeDeVaccin').value,
                    shema: document.getElementById('editShema').value,
                    date_de_certificat: document.getElementById('editDateCertificat').value,
                    date_de_naissance: document.getElementById('editDateNaissance').value || null,
                    animal: document.getElementById('editAnimal').value
                };
            }
            
            const response = await fetch('http://127.0.0.1:5000/api/modifier_enregistrement', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    table: table,
                    data: updateData
                })
            });
            
            const result = await response.json();
            
            if (result.success) {
                showSuccess('Enregistrement modifié avec succès');
                document.body.removeChild(modal);
                // Rafraîchir les données
                const tableSelect = document.getElementById('table');
                const dateDebut = document.getElementById('dateDebut').value;
                const dateFin = document.getElementById('dateFin').value;
                await recupererDonnees(tableSelect.value, dateDebut, dateFin);
            } else {
                showError(result.error || 'Erreur lors de la modification');
            }
        } catch (error) {
            console.error('Erreur:', error);
            showError('Erreur de connexion au serveur');
        }
    }
    
    // Fonction pour supprimer un enregistrement
    async function supprimerEnregistrement(data, table) {
        if (!confirm(`Êtes-vous sûr de vouloir supprimer cet enregistrement ?\n\n${data.nom || ''} ${data.prenom || ''}`)) {
            return;
        }
        
        try {
            const response = await fetch('http://127.0.0.1:5000/api/supprimer_enregistrement', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    table: table,
                    id: data.id
                })
            });
            
            const result = await response.json();
            
            if (result.success) {
                showSuccess('Enregistrement supprimé avec succès');
                // Rafraîchir les données
                const tableSelect = document.getElementById('table');
                const dateDebut = document.getElementById('dateDebut').value;
                const dateFin = document.getElementById('dateFin').value;
                await recupererDonnees(tableSelect.value, dateDebut, dateFin);
            } else {
                showError(result.error || 'Erreur lors de la suppression');
            }
        } catch (error) {
            console.error('Erreur:', error);
            showError('Erreur de connexion au serveur');
        }
    }
    
    // Fonction pour exporter vers Excel
    exportExcelBtn.addEventListener('click', function() {
        if (currentData.length === 0) {
            showError('Aucune donnée à exporter');
            return;
        }
        
        // Afficher un message pour indiquer que toutes les données seront exportées
        const totalItems = currentData.length;
        const currentRowsPerPage = window.currentRowsPerPage || rowsPerPage;
        const currentPageItems = Math.min(currentRowsPerPage, totalItems - (currentPage - 1) * currentRowsPerPage);
        
        console.log(`Exportation de ${totalItems} enregistrements (page actuelle: ${currentPageItems}/${currentRowsPerPage})`);
        
        // Vérifier si la bibliothèque XLSX est chargée
        if (typeof XLSX === 'undefined') {
            // Fallback vers CSV
            exportToCSV();
            return;
        }
        
        try {
            // Préparer les données pour Excel
            const table = document.getElementById('table').value;
            let ws_data = [];
            
            // Ajouter les en-têtes
            if (table === 'arrets_travail' || table === 'prolongation') {
                ws_data.push(['Nom', 'Prénom', 'Médecin', 'Nombre de jours', 'Date certificat', 'Date naissance', 'Âge', 'Créé le']);
                currentData.forEach(row => {
                    ws_data.push([
                        row.nom || '',
                        row.prenom || '',
                        row.medecin || '',
                        row.nombre_jours || '',
                        row.date_certificat || '',
                        row.date_naissance || '',
                        row.age || '',
                        row.created_at || ''
                    ]);
                });
            } else if (table === 'cbv') {
                ws_data.push(['Nom', 'Prénom', 'Médecin', 'Date certificat', 'Heure', 'Date naissance', 'Titre', 'Examen', 'Créé le']);
                currentData.forEach(row => {
                    ws_data.push([
                        row.nom || '',
                        row.prenom || '',
                        row.medecin || '',
                        row.date_certificat || '',
                        row.heure || '',
                        row.date_naissance || '',
                        row.titre || '',
                        row.examen || '',
                        row.created_at || ''
                    ]);
                });
            } else if (table === 'antirabique') {
                ws_data.push(['Nom', 'Prénom', 'Médecin', 'Classe', 'Type vaccin', 'Schéma', 'Date certificat', 'Date naissance', 'Animal', 'Créé le']);
                currentData.forEach(row => {
                    ws_data.push([
                        row.nom || '',
                        row.prenom || '',
                        row.medecin || '',
                        row.classe || '',
                        row.type_de_vaccin || '',
                        row.shema || '',
                        row.date_de_certificat || '',
                        row.date_de_naissance || '',
                        row.animal || '',
                        row.created_at || ''
                    ]);
                });
            }
            
            // Créer le workbook et la worksheet
            const ws = XLSX.utils.aoa_to_sheet(ws_data);
            const wb = XLSX.utils.book_new();
            XLSX.utils.book_append_sheet(wb, ws, 'Données');
            
            // Générer le nom du fichier avec date
            const dateStr = new Date().toISOString().split('T')[0];
            const fileName = `donnees_${table}_${dateStr}.xlsx`;
            
            // Télécharger le fichier
            XLSX.writeFile(wb, fileName);
            showSuccess(`Fichier ${fileName} téléchargé avec succès`);
            
        } catch (error) {
            console.error('Erreur Excel:', error);
            // Fallback vers CSV
            exportToCSV();
        }
    });
    
    // Fonction pour exporter vers CSV (fallback)
    function exportToCSV() {
        const table = document.getElementById('table').value;
        let csv = '';
        
        // Ajouter les en-têtes
        if (table === 'arrets_travail' || table === 'prolongation') {
            csv += 'Nom,Prénom,Médecin,Nombre de jours,Date certificat,Date naissance,Âge,Créé le\n';
            currentData.forEach(row => {
                csv += `"${row.nom || ''}","${row.prenom || ''}","${row.medecin || ''}","${row.nombre_jours || ''}","${row.date_certificat || ''}","${row.date_naissance || ''}","${row.age || ''}","${row.created_at || ''}"\n`;
            });
        } else if (table === 'cbv') {
            csv += 'Nom,Prénom,Médecin,Date certificat,Heure,Date naissance,Titre,Examen,Créé le\n';
            currentData.forEach(row => {
                csv += `"${row.nom || ''}","${row.prenom || ''}","${row.medecin || ''}","${row.date_certificat || ''}","${row.heure || ''}","${row.date_naissance || ''}","${row.titre || ''}","${row.examen || ''}","${row.created_at || ''}"\n`;
            });
        } else if (table === 'antirabique') {
            csv += 'Nom,Prénom,Médecin,Classe,Type vaccin,Schéma,Date certificat,Date naissance,Animal,Créé le\n';
            currentData.forEach(row => {
                csv += `"${row.nom || ''}","${row.prenom || ''}","${row.medecin || ''}","${row.classe || ''}","${row.type_de_vaccin || ''}","${row.shema || ''}","${row.date_de_certificat || ''}","${row.date_de_naissance || ''}","${row.animal || ''}","${row.created_at || ''}"\n`;
            });
        }
        
        // Télécharger le fichier CSV
        const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        const dateStr = new Date().toISOString().split('T')[0];
        link.href = URL.createObjectURL(blob);
        link.download = `donnees_${table}_${dateStr}.csv`;
        link.click();
        
        showSuccess(`Fichier CSV téléchargé avec succès`);
    }
    
    // Fonction pour imprimer le tableau
    printTableBtn.addEventListener('click', function() {
        if (currentData.length === 0) {
            showError('Aucune donnée à imprimer');
            return;
        }
        
        window.print();
    });
    
    // Fonctions utilitaires
    function showSuccess(message) {
        successMessage.textContent = message;
        successMessage.style.display = 'block';
        errorMessage.style.display = 'none';
        setTimeout(() => {
            successMessage.style.display = 'none';
        }, 5000);
    }
    
    function showError(message) {
        errorMessage.textContent = message;
        errorMessage.style.display = 'block';
        successMessage.style.display = 'none';
        setTimeout(() => {
            errorMessage.style.display = 'none';
        }, 5000);
    }
});