import { useState, useEffect } from "react";
import { Switch, Route, useLocation } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { PinAuth } from "@/components/auth/pin-auth";
import ReportsPage from "@/pages/reports";
import SalesOrdersPage from "@/pages/sales-orders";
import NotFoundPage from "./pages/not-found";

function Router({ onLogout }: { onLogout: () => void }) {
  const RedirectToReports = () => {
    const [, setLocation] = useLocation();

    useEffect(() => {
      setLocation('/reports', { replace: true });
    }, [setLocation]);

    return null;
  };

  return (
    <Switch>
      <Route path="/" component={RedirectToReports} />
      <Route
        path="/reports"
        component={() => <ReportsPage onLogout={onLogout} />}
      />
      <Route
        path="/sales-orders"
        component={() => <SalesOrdersPage onLogout={onLogout} />}
      />
      <Route path="*" component={NotFoundPage} />
    </Switch>
  );
}

function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  const handleAuthSuccess = () => {
    setIsAuthenticated(true);
  };

  const handleLogout = () => {
    setIsAuthenticated(false);
  };

  // Check if current path is customer display to bypass authentication
  // This logic is removed as per the intention to only keep reporting.
  // const isCustomerDisplay = window.location.pathname === "/customer-display";

  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <Toaster />
        {!isAuthenticated ? ( // Removed !isCustomerDisplay check
          <PinAuth onAuthSuccess={handleAuthSuccess} />
        ) : (
          <Router onLogout={handleLogout} />
        )}
      </TooltipProvider>
    </QueryClientProvider>
  );
}

export default App;