import React, { useState, useEffect } from "react";
import { HelpCircle } from "lucide-react"; // icon library

export default function ModalWidgetPersistent({ url, buttonLabel = "Open Widget" }) {
  const [open, setOpen] = useState(false);
  const [expanded, setExpanded] = useState(false); // user-controlled expansion

  // Close modal on Escape key
  useEffect(() => {
    if (!open) return;
    const handleKeyDown = (e) => {
      if (e.key === "Escape") {
        setOpen(false);
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [open]);

  return (
    <>
      {/* Floating Button: Icon + optional label */}
      <div className="fixed bottom-4 right-4 z-50 flex items-center space-x-2">
        {expanded && (
          <button
            className="modal-launch-btn"
            onClick={() => setOpen(true)}
          >
            {buttonLabel}
          </button>
        )}
        <button
          className="help-icon-btn"
          onClick={() => setExpanded(!expanded)}
          aria-label="Toggle Help Button"
        >
          <HelpCircle size={24} />
        </button>
      </div>

      {/* Full-Screen Modal */}
      <div
        className={`fixed inset-0 z-50 modal-overlay flex flex-col transition-opacity duration-300 ${
          open ? "opacity-100 visible" : "opacity-0 invisible"
        }`}
      >
        {/* Close Button */}
        <div className="absolute top-4 right-4 z-60">
          <button
            className="modal-close-btn"
            onClick={() => setOpen(false)}
          >
            ✕ Close
          </button>
        </div>

        {/* Persistent iframe */}
        <iframe
          src={url}
          title="Modal Widget"
          className="flex-1 w-full h-full border-0"
        />
      </div>
    </>
  );
}
