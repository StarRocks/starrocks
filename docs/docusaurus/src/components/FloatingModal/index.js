// src/components/FloatingModal/index.js
import React, { useState } from 'react';
import styles from './styles.module.css';

export default function FloatingModal() {
  const [isOpen, setIsOpen] = useState(false);

  const toggleModal = () => {
    setIsOpen(!isOpen);
  };

  return (
    <>
      {/* Floating button that opens the modal */}
      <button className={styles.floatingButton} onClick={toggleModal}>
        Feedback
      </button>

      {/* Modal overlay and content */}
      {isOpen && (
        <div className={styles.modalOverlay} onClick={toggleModal}>
          <div className={styles.modalContent} onClick={e => e.stopPropagation()}>
            <button className={styles.closeButton} onClick={toggleModal}>
              &times;
            </button>
            {<iframe src="https://ai-agent.starrocks.com/" width="1200px" height="1200px"></iframe>}
          </div>
        </div>
      )}
    </>
  );
}
