/**
 * ClientEngine — Frontend JavaScript
 */

// ── Modal Toggle ──────────────────────────────────────
function toggleModal(id) {
    const modal = document.getElementById(id);
    if (modal) {
        modal.classList.toggle('active');
    }
}

// Close modal on Escape key
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        document.querySelectorAll('.modal.active').forEach(m => m.classList.remove('active'));
    }
});

// ── Auto-dismiss alerts after 5 seconds ───────────────
document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('.alert').forEach(alert => {
        setTimeout(() => {
            alert.style.opacity = '0';
            alert.style.transform = 'translateY(-8px)';
            setTimeout(() => alert.remove(), 300);
        }, 5000);
    });
});

// ── Confirm dangerous actions ─────────────────────────
document.querySelectorAll('[data-confirm]').forEach(el => {
    el.addEventListener('click', (e) => {
        if (!confirm(el.dataset.confirm)) {
            e.preventDefault();
        }
    });
});
