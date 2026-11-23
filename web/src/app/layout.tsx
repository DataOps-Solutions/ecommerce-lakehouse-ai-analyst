import type { Metadata } from "next";
import type { ReactNode } from "react";

import { Shell } from "@/components/shell";

import "./globals.css";

export const metadata: Metadata = {
  title: "DataOps Solutions · Plataforma lakehouse",
  description: "Resumen, ingesta, corridas, catálogo y analista BI para e-commerce.",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="es" className="h-full antialiased">
      <body className="min-h-full">
        <Shell>{children}</Shell>
      </body>
    </html>
  );
}
