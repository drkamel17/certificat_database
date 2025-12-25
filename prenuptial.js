// الحقول التي يجب حفظها فقط (معلومات الطبيب ومكان إصدار الشهادة)
const PERSISTENT_FIELDS = [
    'doctor-name',        // الاسم واللقب
    'practice-location',  // ممارس في
    'doctor-address',     // العنوان
    'certificate-place'   // حرر بـ
];

// حفظ البيانات في localStorage (الحقول المحددة فقط) - يدوي فقط
function saveData() {
    const data = {};
    
    PERSISTENT_FIELDS.forEach(fieldName => {
        const field = document.querySelector(`[data-field="${fieldName}"]`);
        if (field) {
            data[fieldName] = field.textContent.trim();
        }
    });
    
    localStorage.setItem('prenuptial_certificate_data', JSON.stringify(data));
    
    // إظهار رسالة حفظ
    showMessage('تم حفظ بيانات الطبيب بنجاح! (حفظ يدوي)', 'success');
}

// تحميل البيانات من localStorage (الحقول المحددة فقط)
function loadData() {
    const savedData = localStorage.getItem('prenuptial_certificate_data');
    
    if (savedData) {
        const data = JSON.parse(savedData);
        
        PERSISTENT_FIELDS.forEach(fieldName => {
            const field = document.querySelector(`[data-field="${fieldName}"]`);
            if (field && data[fieldName]) {
                field.textContent = data[fieldName];
                field.classList.remove('empty');
            }
        });
        
        showMessage('تم تحميل بيانات الطبيب بنجاح!', 'info');
    }
}

// مسح البيانات (اختيار بين مسح الكل أو بيانات الطبيب فقط)
function clearData() {
    const choice = confirm('اختر نوع المسح:\n\nموافق = مسح بيانات الطبيب المحفوظة فقط\nإلغاء = مسح جميع الحقول');
    
    if (choice) {
        // مسح بيانات الطبيب المحفوظة فقط
        PERSISTENT_FIELDS.forEach(fieldName => {
            const field = document.querySelector(`[data-field="${fieldName}"]`);
            if (field) {
                field.textContent = '';
                field.classList.add('empty');
            }
        });
        
        localStorage.removeItem('prenuptial_certificate_data');
        showMessage('تم مسح بيانات الطبيب المحفوظة!', 'warning');
    } else {
        // مسح جميع الحقول
        const fields = document.querySelectorAll('.editable-field');
        fields.forEach(field => {
            field.textContent = '';
            field.classList.add('empty');
        });
        
        showMessage('تم مسح جميع الحقول!', 'warning');
    }
}

// إظهار رسائل التأكيد
function showMessage(message, type) {
    // إنشاء عنصر الرسالة
    const messageEl = document.createElement('div');
    messageEl.textContent = message;
    messageEl.style.cssText = `
        position: fixed;
        top: 60px;
        left: 10px;
        background: ${type === 'success' ? '#27ae60' : type === 'warning' ? '#f39c12' : '#3498db'};
        color: white;
        padding: 10px 15px;
        border-radius: 5px;
        z-index: 1001;
        font-size: 14px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.2);
    `;
    
    document.body.appendChild(messageEl);
    
    // إزالة الرسالة بعد 3 ثوان
    setTimeout(() => {
        document.body.removeChild(messageEl);
    }, 3000);
}

// تعيين التاريخ الحالي
function setCurrentDate() {
    const dateField = document.querySelector('[data-field="certificate-date"]');
    if (dateField) {
        // تعيين التاريخ الميلادي الحالي بالأرقام العادية دائماً عند تحميل الصفحة
        const today = new Date();
        const day = String(today.getDate()).padStart(2, '0');
        const monthNames = [
            'يناير', 'فبراير', 'مارس', 'أبريل', 'مايو', 'يونيو',
            'يوليو', 'أغسطس', 'سبتمبر', 'أكتوبر', 'نوفمبر', 'ديسمبر'
        ];
        const month = monthNames[today.getMonth()];
        const year = today.getFullYear();
        
        const gregorianDate = `${day} ${month} ${year}`;
        dateField.textContent = gregorianDate;
        dateField.classList.remove('empty');
    }
}

// تحميل البيانات عند تحميل الصفحة
document.addEventListener('DOMContentLoaded', function() {
    loadData();
    setCurrentDate();
    
    // إضافة مستمعي الأحداث للأزرار
    document.getElementById('saveBtn').addEventListener('click', saveData);
    document.getElementById('loadBtn').addEventListener('click', loadData);
    document.getElementById('clearBtn').addEventListener('click', clearData);
    document.getElementById('printBtn').addEventListener('click', function() {
        // Alerte en français pour les paramètres d'impression
        alert('Veuillez sélectionner le format "A4"  dans les options d\'impression.');
        window.print();
    });
    
    // إضافة مستمعي الأحداث للحقول القابلة للتعديل
    const editableFields = document.querySelectorAll('.editable-field');
    editableFields.forEach(field => {
        // إزالة الحفظ التلقائي - الحفظ يدوي فقط عند الضغط على زر "حفظ طبيب"
        // field.addEventListener('blur', function() {
        //     const fieldName = this.getAttribute('data-field');
        //     if (PERSISTENT_FIELDS.includes(fieldName)) {
        //         saveData();
        //     }
        // });
        
        // إدارة placeholder
        field.addEventListener('focus', function() {
            this.classList.remove('empty');
        });
        
        field.addEventListener('blur', function() {
            if (this.textContent.trim() === '') {
                this.classList.add('empty');
            }
        });
        
        // التحقق من الحالة الأولية
        if (field.textContent.trim() === '') {
            field.classList.add('empty');
        }
    });
});