import React, { useState } from 'react';
import Modal from 'react-modal';
import { ResizableBox } from 'react-resizable';
import './FloatingButtonWithModal.css';

// Bind modal to your app element
Modal.setAppElement('#__docusaurus');

export default function FloatingButtonWithModal({
  modalTitle,
  modalDisclaimer,
  iframeSrc,
  initialWidth = 1000,
  initialHeight = 500,
}) {
  const [modalIsOpen, setIsOpen] = useState(false);
  const [width, setWidth] = useState(initialWidth);
  const [height, setHeight] = useState(initialHeight);

  function openModal() {
    setIsOpen(true);
  }

  function closeModal() {
    setIsOpen(false);
  }

  function onResize(event, { size }) {
    setWidth(size.width);
    setHeight(size.height);
  }

  return (
    <>
      <button onClick={openModal} className="floating-action-button">
        Ask the Agent
      </button>

      <Modal
        isOpen={modalIsOpen}
        onRequestClose={closeModal}
        contentLabel={modalTitle}
        className="ResizableIframeModal-Content"
        overlayClassName="ResizableIframeModal-Overlay"
      >
        <ResizableBox
          width={width}
          height={height}
          onResize={onResize}
          minConstraints={[400, 300]} // Set a minimum size
          maxConstraints={[1200, 800]} // Set a maximum size
        >
          <div className="modal-header">
            <h3>{modalTitle}</h3>
            <button onClick={closeModal} className="close-button">&times;</button>
          </div>
          <div>
            <p>{modalDisclaimer}</p>
          </div>
          <iframe
            src={iframeSrc}
            title={modalTitle}
            width="100%"
            height="100%"
            style={{ border: 'none' }}
          />
        </ResizableBox>
      </Modal>
    </>
  );
}
