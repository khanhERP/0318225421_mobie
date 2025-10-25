import React from "react";
import { useTranslation } from "@/lib/i18n";
import { Button } from "@/components/ui/button";
import { LogOut, Menu } from "lucide-react";
import logoPath from "@assets/EDPOS_1753091767028.png";

interface ReportsHeaderProps {
  onLogout?: () => void;
  title?: string;
}

export function ReportsHeader({
  onLogout,
  title = "DASHBOARD",
}: ReportsHeaderProps) {
  const { t } = useTranslation();

  const handleLogout = () => {
    // Clear authentication state from sessionStorage
    sessionStorage.removeItem("pinAuthenticated");
    // Call callback onLogout if provided
    if (onLogout) {
      onLogout();
    }
    // Reload page to return to login screen
    window.location.reload();
  };

  // Get current date
  const currentDate = new Date().toLocaleDateString("vi-VN", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  });

  return (
    <header className="fixed top-0 left-0 right-0 bg-green-500 text-white shadow-lg z-40 h-16">
      <div className="max-w-full mx-auto px-4 h-full flex items-center justify-between">
        {/* Left side - Dynamic title */}
        <div className="flex flex-col items-end">
          <h1 className="text-xl font-bold text-white">{title}</h1>
          <span className="text-[0.819rem] text-white opacity-90">매장이름</span>
        </div>

        {/* Center - Date info */}
        <div className="flex items-center text-sm">
          <span>{t("reports.toDay")}</span>
        </div>

        {/* Right side - Logo */}
        <div className="flex items-center">
          <img
            src={logoPath}
            alt="EDPOS Logo"
            className="h-8 md:h-12 object-contain"
            onError={(e) => {
              console.error("Failed to load logo image");
              e.currentTarget.style.display = "none";
            }}
          />
        </div>
      </div>
    </header>
  );
}
