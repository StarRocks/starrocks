import React from "react";
import ModalWidgetPersistent from "@site/src/components/ModalWidgetPersistent";

export default function Root({ children }) {
  return (
    <>
      {children}
      {/* Floating widget trigger in bottom-right, always visible */}
      <div className="fixed bottom-4 right-4 z-50">
        <ModalWidgetPersistent
          url="https://example.com/app"
          buttonLabel="Help"
        />
      </div>
    </>
  );
}
