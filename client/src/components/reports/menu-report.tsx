import { useState, useEffect } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "@/lib/queryClient";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  BarChart3,
  TrendingUp,
  Package,
  DollarSign,
  Search,
  RefreshCw,
  ArrowLeft,
  Filter,
  Calendar,
  ChevronRight,
} from "lucide-react";
import { useTranslation } from "@/lib/i18n";
import { format, subDays, startOfMonth, endOfMonth } from "date-fns";
import { vi } from "date-fns/locale";
import logoPath from "@assets/EDPOS_1753091767028.png";

interface Product {
  id: number;
  name: string;
  sku: string;
  price: string;
  stock: number;
  categoryId: number;
  categoryName?: string;
  productType: number;
  taxRate: string;
  isActive: boolean;
}

interface Category {
  id: number;
  name: string;
  icon: string;
}

interface MenuAnalysisData {
  totalRevenue: number;
  totalQuantity: number;
  categoryStats: Array<{
    categoryId: number;
    categoryName: string;
    totalQuantity: number;
    totalRevenue: number;
    productCount: number;
  }>;
  productStats: Array<{
    productId: number;
    productName: string;
    totalQuantity: number;
    totalRevenue: number;
    averagePrice: number;
  }>;
  topSellingProducts: Array<{
    productId: number;
    productName: string;
    totalQuantity: number;
    totalRevenue: number;
  }>;
  topRevenueProducts: Array<{
    productId: number;
    productName: string;
    totalQuantity: number;
    totalRevenue: number;
  }>;
}

interface MenuReportProps {
  onBack: () => void;
  onReportsClick?: () => void;
  onDailySalesClick?: () => void;
}

export function MenuReport({
  onBack,
  onReportsClick,
  onDailySalesClick,
}: MenuReportProps) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const [dateRange, setDateRange] = useState(() => {
    const today = new Date();
    const formattedToday = format(today, "yyyy-MM-dd");
    return { start: formattedToday, end: formattedToday };
  });
  const [selectedCategory, setSelectedCategory] = useState<string>("all");
  const [productType, setProductType] = useState<string>("all");
  const [productSearch, setProductSearch] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const [showDatePicker, setShowDatePicker] = useState(false);
  const [activeFilter, setActiveFilter] = useState("today");
  const itemsPerPage = 20;

  // Query menu analysis data
  const {
    data: menuAnalysis,
    isLoading: analysisLoading,
    error: analysisError,
    refetch,
  } = useQuery({
    queryKey: [
      "https://09978332-5dc6-4a9a-8375-fec123be89da-00-1qhtnuziydfl4.pike.replit.dev/menu-analysis",
      dateRange.start,
      dateRange.end,
      selectedCategory,
    ],
    queryFn: async () => {
      try {
        const params = new URLSearchParams({
          startDate: dateRange.start,
          endDate: dateRange.end,
          ...(selectedCategory !== "all" && { categoryId: selectedCategory }),
        });

        const response = await apiRequest(
          "GET",
          `https://09978332-5dc6-4a9a-8375-fec123be89da-00-1qhtnuziydfl4.pike.replit.dev/menu-analysis?${params.toString()}`,
        );
        if (!response.ok) {
          throw new Error(`Failed to fetch menu analysis: ${response.status}`);
        }

        const data = await response.json();
        return {
          totalRevenue: Number(data.totalRevenue || 0),
          totalQuantity: Number(data.totalQuantity || 0),
          categoryStats: Array.isArray(data.categoryStats)
            ? data.categoryStats
            : [],
          productStats: Array.isArray(data.productStats)
            ? data.productStats
            : [],
          topSellingProducts: Array.isArray(data.topSellingProducts)
            ? data.topSellingProducts
            : [],
          topRevenueProducts: Array.isArray(data.topRevenueProducts)
            ? data.topRevenueProducts
            : [],
        } as MenuAnalysisData;
      } catch (error) {
        console.error("Error fetching menu analysis:", error);
        return {
          totalRevenue: 0,
          totalQuantity: 0,
          categoryStats: [],
          productStats: [],
          topSellingProducts: [],
          topRevenueProducts: [],
        } as MenuAnalysisData;
      }
    },
    retry: 2,
    retryDelay: 1000,
  });

  const formatCurrency = (
    amount: number | string | undefined | null,
  ): string => {
    const num = typeof amount === "string" ? parseFloat(amount) : amount;
    if (typeof num !== "number" || isNaN(num)) {
      return "0";
    }
    return Math.floor(num).toLocaleString("vi-VN");
  };

  const handleRefresh = () => {
    queryClient.invalidateQueries({ queryKey: ["https://09978332-5dc6-4a9a-8375-fec123be89da-00-1qhtnuziydfl4.pike.replit.dev/menu-analysis"] });
    refetch();
  };

  const formatDate = (dateStr: string) => {
    try {
      const date = new Date(dateStr);
      return format(date, "dd/MM/yyyy", { locale: vi });
    } catch {
      return dateStr;
    }
  };

  // Filter and sort products for display
  const filteredAndSortedProducts = () => {
    if (!menuAnalysis?.productStats) return [];

    const filtered = menuAnalysis.productStats.filter(
      (product) =>
        !productSearch ||
        (product.productName &&
          product.productName
            .toLowerCase()
            .includes(productSearch.toLowerCase())),
    );

    return filtered.sort(
      (a, b) => (b.totalRevenue || 0) - (a.totalRevenue || 0),
    );
  };

  const totalPages = Math.ceil(
    filteredAndSortedProducts().length / itemsPerPage,
  );
  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  const currentProducts = filteredAndSortedProducts().slice(
    startIndex,
    endIndex,
  );

  if (analysisLoading) {
    return (
      <div className="flex justify-center items-center min-h-[400px]">
        <div className="text-lg text-gray-500">{t("reports.loading")}</div>
      </div>
    );
  }

  if (analysisError) {
    return (
      <div className="flex justify-center items-center min-h-[400px] text-red-500">
        {t("reports.errorLoadingData")}:{" "}
        {analysisError.message || t("common.unknown")}
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-green-50">
      {/* Header */}
      <div className="bg-green-600 text-white p-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Button
            variant="ghost"
            size="icon"
            className="text-white hover:bg-green-700"
            onClick={onBack}
          >
            <ArrowLeft className="w-5 h-5" />
          </Button>
          <h1 className="text-lg font-semibold">{t("reports.menuAnalysis")}</h1>
        </div>
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

      {/* Date Filter Section */}
      <div className="bg-gray-200 p-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Calendar className="w-5 h-5 text-gray-600" />
            <Button
              variant="ghost"
              size="sm"
              className="text-sm text-gray-700 hover:bg-gray-300 px-2 py-1"
              onClick={() => setShowDatePicker(!showDatePicker)}
            >
              {formatDate(dateRange.start)} - {formatDate(dateRange.end)}
              <ChevronRight
                className={`w-4 h-4 ml-1 transition-transform ${showDatePicker ? "rotate-90" : ""}`}
              />
            </Button>
          </div>
          <div className="flex items-center gap-2">
            <Filter className="w-4 h-4 text-gray-600" />
          </div>
        </div>

        {/* Date Range Picker */}
        {showDatePicker && (
          <div className="mt-3 p-3 bg-white rounded-lg shadow-sm border">
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="text-xs text-gray-600 block mb-1">
                  {t("reports.startDate")}
                </label>
                <input
                  type="date"
                  value={dateRange.start}
                  onChange={(e) =>
                    setDateRange((prev) => ({ ...prev, start: e.target.value }))
                  }
                  className="w-full text-sm border border-gray-300 rounded px-2 py-1"
                />
              </div>
              <div>
                <label className="text-xs text-gray-600 block mb-1">
                  {t("reports.endDate")}
                </label>
                <input
                  type="date"
                  value={dateRange.end}
                  onChange={(e) =>
                    setDateRange((prev) => ({ ...prev, end: e.target.value }))
                  }
                  className="w-full text-sm border border-gray-300 rounded px-2 py-1"
                />
              </div>
            </div>
            <div className="flex gap-2 mt-3">
              <Button
                size="sm"
                onClick={() => {
                  const today = new Date();
                  const formattedToday = format(today, "yyyy-MM-dd");
                  setDateRange({ start: formattedToday, end: formattedToday });
                  setActiveFilter("today");
                }}
                className={`text-xs px-3 py-1 ${
                  activeFilter === "today"
                    ? "bg-green-600 hover:bg-green-700 text-white"
                    : "border border-green-600 text-green-600 bg-white hover:bg-green-50"
                }`}
              >
                {t("reports.toDay")}
              </Button>
              <Button
                size="sm"
                onClick={handleRefresh}
                className="text-xs px-3 py-1 bg-green-600 hover:bg-green-700 text-white"
              >
                {t("reports.refresh")}
              </Button>
            </div>
          </div>
        )}
      </div>

      {/* Content */}
      <div className="p-4">
        {/* Main Table */}
        <Card>
          <CardHeader>
            <CardTitle>{t("reports.menuAnalysis")}</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <div className="min-w-full">
                <table className="w-full">
                  <thead>
                    <tr className="bg-gray-100 border-b border-gray-200">
                      <th className="text-left py-3 px-3 text-sm font-medium text-gray-700">
                        #
                      </th>
                      <th className="text-left py-3 px-3 text-sm font-medium text-gray-700">
                        {t("reports.productName")}
                      </th>
                      <th className="text-center py-3 px-3 text-sm font-medium text-gray-700">
                        {t("reports.quantity")}
                      </th>
                      <th className="text-right py-3 px-3 text-sm font-medium text-gray-700">
                        {t("reports.totalRevenue")}
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {currentProducts.length > 0 ? (
                      currentProducts.map((product, index) => {
                        const colors = [
                          "bg-red-500 text-white",
                          "bg-green-500 text-white",
                          "bg-yellow-500 text-white",
                          "bg-blue-500 text-white",
                          "bg-purple-500 text-white",
                          "bg-pink-500 text-white",
                          "bg-indigo-500 text-white",
                          "bg-orange-500 text-white",
                          "bg-teal-500 text-white",
                          "bg-cyan-500 text-white",
                          "bg-emerald-500 text-white",
                          "bg-rose-500 text-white",
                          "bg-violet-500 text-white",
                          "bg-amber-500 text-white",
                          "bg-lime-500 text-white",
                        ];
                        const colorClass =
                          colors[(startIndex + index) % colors.length] ||
                          "bg-gray-500 text-white";

                        return (
                          <tr
                            key={`${product.productId}-${index}`}
                            className="border-b border-gray-200"
                          >
                            <td className="py-3 px-3">
                              <div
                                className={`w-6 h-6 rounded-sm flex items-center justify-center text-xs font-bold ${colorClass}`}
                              >
                                {startIndex + index + 1}
                              </div>
                            </td>
                            <td className="py-3 px-3">
                              <div className="text-sm text-gray-900">
                                {product.productName ||
                                  `Sản phẩm ${product.productId}`}
                              </div>
                            </td>
                            <td className="py-3 px-3 text-center">
                              <span className="text-sm text-gray-900">
                                {(product.totalQuantity || 0).toLocaleString(
                                  "vi-VN",
                                )}
                              </span>
                            </td>
                            <td className="py-3 px-3 text-right">
                              <span className="text-sm font-medium text-gray-900">
                                {formatCurrency(product.totalRevenue || 0)}
                              </span>
                            </td>
                          </tr>
                        );
                      })
                    ) : (
                      <tr>
                        <td
                          colSpan={4}
                          className="py-8 text-center text-gray-500"
                        >
                          {t("reports.noData")}
                        </td>
                      </tr>
                    )}
                  </tbody>
                  {/* Total Row */}
                  {menuAnalysis && (
                    <tfoot>
                      <tr className="bg-gray-100 font-bold border-t-2 border-gray-300">
                        <td className="py-3 px-3"></td>
                        <td className="py-3 px-3 text-sm font-bold text-gray-900">
                          {t("reports.total")}
                        </td>
                        <td className="py-3 px-3 text-center text-sm font-bold text-gray-900">
                          {menuAnalysis.totalQuantity?.toLocaleString(
                            "vi-VN",
                          ) || "0"}
                        </td>
                        <td className="py-3 px-3 text-right text-sm font-bold text-gray-900">
                          {formatCurrency(
                            menuAnalysis.productStats.reduce((sum, product) => {
                              return sum + (product.totalRevenue || 0);
                            }, 0),
                          )}
                        </td>
                      </tr>
                    </tfoot>
                  )}
                </table>
              </div>
            </div>

            {/* Pagination */}
            {totalPages > 1 && (
              <div className="flex justify-center items-center gap-2 mt-4">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setCurrentPage(Math.max(1, currentPage - 1))}
                  disabled={currentPage === 1}
                >
                  {t("common.previous")}
                </Button>
                <span className="text-sm text-gray-600">
                  {t("common.page")} {currentPage} / {totalPages}
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() =>
                    setCurrentPage(Math.min(totalPages, currentPage + 1))
                  }
                  disabled={currentPage === totalPages}
                >
                  {t("common.next")}
                </Button>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
